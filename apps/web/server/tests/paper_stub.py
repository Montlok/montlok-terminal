class RunView:
    def __init__(self, path):
        self.path = path

    def snapshot(self):
        return {"runId": self.path.name, "positions": [], "orders": [], "fills": []}
