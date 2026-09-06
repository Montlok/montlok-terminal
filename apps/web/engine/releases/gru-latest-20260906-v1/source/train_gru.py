"""Train one GRU on preprocessed arrays; select weights using validation loss only."""
import os
os.environ.setdefault("CUBLAS_WORKSPACE_CONFIG", ":4096:8")

import argparse
import json
import random
import time
from pathlib import Path

import numpy as np
import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset


class ReturnGRU(nn.Module):
    def __init__(self, inputs, hidden=64):
        super().__init__()
        self.gru = nn.GRU(inputs, hidden, batch_first=True)
        self.head = nn.Sequential(nn.LayerNorm(hidden), nn.Linear(hidden, 1))

    def forward(self, x):
        _, hidden = self.gru(x)
        return self.head(hidden[-1]).squeeze(-1)


def make_sequences(data, indices):
    length = int(data["sequence"])
    offsets = indices[:, None] + np.arange(1 - length, 1)[None, :]
    return torch.from_numpy(data["x"][offsets])


def predict(model, x, device):
    model.eval()
    with torch.no_grad():
        return np.concatenate([model(batch.to(device)).cpu().numpy() for batch in x.split(1024)]) / 1000.0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--epochs", type=int, default=20)
    parser.add_argument("--refit", action="store_true", help="Fixed-epoch latest-data fit; no holdout claims")
    args = parser.parse_args()
    seed = 7
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.set_num_threads(4)
    torch.use_deterministic_algorithms(True)
    torch.backends.cudnn.benchmark = False
    device = torch.device(args.device)
    if device.type == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA requested but unavailable")
    data = np.load(args.data_dir / "dataset.npz", allow_pickle=False)
    train_x = make_sequences(data, data["train"])
    valid_x = None if args.refit else make_sequences(data, data["valid"])
    train_y = torch.from_numpy(data["y"][data["train"]] * 1000)
    valid_y = data["y"][data["valid"]]
    model = ReturnGRU(data["x"].shape[1]).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=0.001, weight_decay=0.01)
    loss_fn = nn.HuberLoss(delta=1.0)
    loader = DataLoader(TensorDataset(train_x, train_y), batch_size=256, shuffle=True,
                        generator=torch.Generator().manual_seed(seed), num_workers=0)
    best_loss, best_state, stale, history = float("inf"), None, 0, []
    started = time.monotonic()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for epoch in range(1, args.epochs + 1):
        model.train()
        total = 0.0
        for x, y in loader:
            x, y = x.to(device), y.to(device)
            optimizer.zero_grad(set_to_none=True)
            loss = loss_fn(model(x), y)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            total += float(loss.detach()) * len(x)
        val_pred = None if args.refit else predict(model, valid_x, device)
        val_mse = None if args.refit else float(np.mean((val_pred - valid_y) ** 2))
        row = {"epoch": epoch, "train_huber_scaled": total / len(train_x),
               "valid_mse": val_mse, "elapsed_seconds": time.monotonic() - started}
        history.append(row)
        print(json.dumps(row), flush=True)
        if args.refit or val_mse < best_loss:
            best_loss = val_mse
            best_state = {name: value.detach().cpu().clone() for name, value in model.state_dict().items()}
            best_epoch = epoch
            stale = 0
        else:
            stale += 1
        if not args.refit and stale >= 4:
            break
    if best_state is None:
        raise RuntimeError("No finite validation checkpoint")
    model.load_state_dict(best_state)
    checkpoint = {"state_dict": best_state, "input_size": data["x"].shape[1], "hidden_size": 64,
                  "sequence_bars": int(data["sequence"]), "target_scale": 1000.0,
                  "feature_mean": torch.from_numpy(data["mean"]), "feature_scale": torch.from_numpy(data["scale"])}
    torch.save(checkpoint, args.output_dir / "model.pt")
    outputs = {}
    for split in (() if args.refit else ("valid", "test")):
        ix = data[split]
        outputs[f"{split}_prediction"] = predict(model, make_sequences(data, ix), device)
        outputs[f"{split}_index"] = ix
    np.savez_compressed(args.output_dir / "predictions.npz", **outputs)
    result = {"model": "ReturnGRU", "hidden_size": 64, "seed": seed, "best_epoch": best_epoch,
              "parameters": sum(p.numel() for p in model.parameters()), "torch": torch.__version__,
              "device": str(device), "gpu": torch.cuda.get_device_name(0) if device.type == "cuda" else None,
              "peak_gpu_allocated_bytes": torch.cuda.max_memory_allocated() if device.type == "cuda" else None,
              "elapsed_seconds": time.monotonic() - started, "history": history,
              "weight_selection": ("fixed epochs selected by prior research run; all eligible latest data used" if args.refit
                                   else "lowest validation MSE; test unused during training"),
              "mode": "latest_refit" if args.refit else "research_holdout"}
    (args.output_dir / "training.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
