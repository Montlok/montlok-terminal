// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Fail-safe launch policy shared by checked-in OKX execution utilities.

use crate::common::enums::OKXEnvironment;

const LIVE_FLATTEN_CONFIRMATION_PREFIX: &str = "OKX-LIVE-FLATTEN";
const LIVE_ORDERS_CONFIRMATION_PREFIX: &str = "OKX-LIVE-ORDERS";

/// Environment values supplied to the OKX launch-policy parser.
///
/// Keeping these values separate from process environment access makes the policy deterministic
/// and testable before any client is constructed or network connection is attempted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OKXLaunchEnvironment<'a> {
    /// Optional `OKX_LIVE` value.
    pub live: Option<&'a str>,
    /// Optional `OKX_DEMO_ORDERS` value.
    pub demo_orders: Option<&'a str>,
    /// Optional `OKX_LIVE_ORDERS` value.
    pub live_orders: Option<&'a str>,
    /// Optional `OKX_LIVE_CONFIRMATION` value.
    pub live_confirmation: Option<&'a str>,
    /// Optional `OKX_EXPECTED_ACCOUNT_ID` value.
    pub expected_account_id: Option<&'a str>,
    /// Account ID configured by the calling execution utility, when it has one.
    pub configured_account_id: Option<&'a str>,
}

/// Validated launch policy for the destructive OKX flatten utility.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OKXFlattenLaunchPolicy {
    /// Selected OKX API environment.
    pub environment: OKXEnvironment,
    /// Operator-supplied account identifier bound into the live confirmation token.
    pub expected_account_id: Option<String>,
}

/// Validated launch policy for an OKX execution example.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OKXExecutionLaunchPolicy {
    /// Selected OKX API environment.
    pub environment: OKXEnvironment,
    /// Whether this invocation may submit orders.
    pub orders_enabled: bool,
    /// Operator-supplied account identifier bound into the live confirmation token.
    pub expected_account_id: Option<String>,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ParsedLaunchArguments {
    live: bool,
    demo_orders: bool,
    live_orders: bool,
    live_confirmation: Option<String>,
    expected_account_id: Option<String>,
}

/// Parses and validates launch policy for the destructive OKX flatten utility.
///
/// With no flags and no environment switches, the returned policy always selects demo. Live
/// flattening requires `--live` (or `OKX_LIVE=true`), an expected account ID, and an exact
/// `OKX-LIVE-FLATTEN:<account-id>` confirmation token.
///
/// # Errors
///
/// Returns an error for unknown or incomplete arguments, invalid boolean environment values, or
/// any live invocation whose account-bound confirmation is absent or mismatched.
pub fn parse_okx_flatten_launch_policy(
    args: &[String],
    environment: OKXLaunchEnvironment<'_>,
) -> anyhow::Result<OKXFlattenLaunchPolicy> {
    let parsed = parse_launch_arguments(args, false)?;
    let environment_live = parse_optional_bool("OKX_LIVE", environment.live)?;
    let live = parsed.live || environment_live;
    let expected_account_id = parsed
        .expected_account_id
        .as_deref()
        .or(environment.expected_account_id);
    let live_confirmation = parsed
        .live_confirmation
        .as_deref()
        .or(environment.live_confirmation);

    if !live {
        return Ok(OKXFlattenLaunchPolicy {
            environment: OKXEnvironment::Demo,
            expected_account_id: None,
        });
    }

    let expected_account_id = validate_live_confirmation(
        "flatten",
        LIVE_FLATTEN_CONFIRMATION_PREFIX,
        expected_account_id,
        live_confirmation,
    )?;
    Ok(OKXFlattenLaunchPolicy {
        environment: OKXEnvironment::Live,
        expected_account_id: Some(expected_account_id.to_string()),
    })
}

/// Parses and validates launch policy for an OKX execution example.
///
/// The default policy selects demo and disables orders. `--demo-orders` (or
/// `OKX_DEMO_ORDERS=true`) explicitly enables demo orders. `--live` selects live connectivity but
/// remains dry-run; live orders additionally require `--live-orders` (or
/// `OKX_LIVE_ORDERS=true`), an expected account ID, and an exact
/// `OKX-LIVE-ORDERS:<account-id>` confirmation token.
///
/// # Errors
///
/// Returns an error for unknown or incomplete arguments, invalid boolean environment values,
/// contradictory order switches, live-order enablement without live connectivity, or absent and
/// mismatched live confirmation.
pub fn parse_okx_execution_launch_policy(
    args: &[String],
    environment: OKXLaunchEnvironment<'_>,
) -> anyhow::Result<OKXExecutionLaunchPolicy> {
    let parsed = parse_launch_arguments(args, true)?;
    let environment_live = parse_optional_bool("OKX_LIVE", environment.live)?;
    let environment_demo_orders = parse_optional_bool("OKX_DEMO_ORDERS", environment.demo_orders)?;
    let environment_live_orders = parse_optional_bool("OKX_LIVE_ORDERS", environment.live_orders)?;
    let live = parsed.live || environment_live;
    let demo_orders = parsed.demo_orders || environment_demo_orders;
    let live_orders = parsed.live_orders || environment_live_orders;

    anyhow::ensure!(
        !(demo_orders && live_orders),
        "--demo-orders and --live-orders are mutually exclusive"
    );
    anyhow::ensure!(
        !(live && demo_orders),
        "demo orders cannot be enabled for a live connection"
    );
    anyhow::ensure!(
        !live_orders || live,
        "live orders require the independent --live connection gate"
    );

    if !live_orders {
        return Ok(OKXExecutionLaunchPolicy {
            environment: if live {
                OKXEnvironment::Live
            } else {
                OKXEnvironment::Demo
            },
            orders_enabled: demo_orders,
            expected_account_id: None,
        });
    }

    let expected_account_id = parsed
        .expected_account_id
        .as_deref()
        .or(environment.expected_account_id);
    let live_confirmation = parsed
        .live_confirmation
        .as_deref()
        .or(environment.live_confirmation);
    let expected_account_id = validate_live_confirmation(
        "orders",
        LIVE_ORDERS_CONFIRMATION_PREFIX,
        expected_account_id,
        live_confirmation,
    )?;
    if let Some(configured_account_id) = environment.configured_account_id {
        anyhow::ensure!(
            expected_account_id == configured_account_id,
            "expected live account {expected_account_id:?} does not match configured account \
             {configured_account_id:?}"
        );
    }

    Ok(OKXExecutionLaunchPolicy {
        environment: OKXEnvironment::Live,
        orders_enabled: true,
        expected_account_id: Some(expected_account_id.to_string()),
    })
}

fn parse_launch_arguments(
    args: &[String],
    allow_order_switches: bool,
) -> anyhow::Result<ParsedLaunchArguments> {
    let mut parsed = ParsedLaunchArguments::default();
    let mut index = 0;

    while index < args.len() {
        let argument = &args[index];
        match argument.as_str() {
            "--live" => parsed.live = true,
            "--demo-orders" if allow_order_switches => parsed.demo_orders = true,
            "--live-orders" if allow_order_switches => parsed.live_orders = true,
            "--live-confirmation" => {
                index += 1;
                parsed.live_confirmation =
                    Some(required_argument_value(args, index, "--live-confirmation")?);
            }
            "--expected-account-id" => {
                index += 1;
                parsed.expected_account_id = Some(required_argument_value(
                    args,
                    index,
                    "--expected-account-id",
                )?);
            }
            "--help" | "-h" => anyhow::bail!(launch_usage(allow_order_switches)),
            _ => {
                if let Some(value) = argument.strip_prefix("--live-confirmation=") {
                    anyhow::ensure!(!value.is_empty(), "--live-confirmation must not be empty");
                    parsed.live_confirmation = Some(value.to_string());
                } else if let Some(value) = argument.strip_prefix("--expected-account-id=") {
                    anyhow::ensure!(!value.is_empty(), "--expected-account-id must not be empty");
                    parsed.expected_account_id = Some(value.to_string());
                } else {
                    anyhow::bail!(
                        "unknown argument {argument:?}; {}",
                        launch_usage(allow_order_switches)
                    );
                }
            }
        }
        index += 1;
    }

    Ok(parsed)
}

fn required_argument_value(
    args: &[String],
    index: usize,
    argument: &str,
) -> anyhow::Result<String> {
    let value = args
        .get(index)
        .ok_or_else(|| anyhow::anyhow!("{argument} requires a value"))?;
    anyhow::ensure!(
        !value.is_empty() && !value.starts_with("--"),
        "{argument} requires a non-empty value"
    );
    Ok(value.clone())
}

fn parse_optional_bool(name: &str, value: Option<&str>) -> anyhow::Result<bool> {
    match value {
        None => Ok(false),
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        Some(value) => anyhow::bail!("{name} must be exactly true or false, got {value:?}"),
    }
}

fn validate_live_confirmation<'a>(
    action: &str,
    confirmation_prefix: &str,
    expected_account_id: Option<&'a str>,
    live_confirmation: Option<&str>,
) -> anyhow::Result<&'a str> {
    let expected_account_id = expected_account_id
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("live {action} requires --expected-account-id"))?;
    let required_confirmation = format!("{confirmation_prefix}:{expected_account_id}");
    anyhow::ensure!(
        live_confirmation == Some(required_confirmation.as_str()),
        "live {action} confirmation must exactly match {required_confirmation:?}"
    );
    Ok(expected_account_id)
}

const fn launch_usage(allow_order_switches: bool) -> &'static str {
    if allow_order_switches {
        "usage: [--live] [--demo-orders | --live-orders] [--expected-account-id ID] \
         [--live-confirmation TOKEN]"
    } else {
        "usage: [--live] [--expected-account-id ID] [--live-confirmation TOKEN]"
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[rstest]
    fn flatten_unset_defaults_to_demo() {
        let policy = parse_okx_flatten_launch_policy(&[], OKXLaunchEnvironment::default()).unwrap();

        assert_eq!(policy.environment, OKXEnvironment::Demo);
        assert!(policy.expected_account_id.is_none());
    }

    #[rstest]
    fn flatten_false_environment_switch_defaults_to_demo() {
        let policy = parse_okx_flatten_launch_policy(
            &[],
            OKXLaunchEnvironment {
                live: Some("false"),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(policy.environment, OKXEnvironment::Demo);
    }

    #[rstest]
    fn flatten_live_without_confirmation_fails_closed() {
        let error =
            parse_okx_flatten_launch_policy(&args(&["--live"]), OKXLaunchEnvironment::default())
                .unwrap_err();

        assert!(error.to_string().contains("expected-account-id"));
    }

    #[rstest]
    fn flatten_live_confirmation_for_wrong_account_is_rejected() {
        let error = parse_okx_flatten_launch_policy(
            &args(&[
                "--live",
                "--expected-account-id",
                "account-a",
                "--live-confirmation",
                "OKX-LIVE-FLATTEN:account-b",
            ]),
            OKXLaunchEnvironment::default(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("OKX-LIVE-FLATTEN:account-a"));
    }

    #[rstest]
    fn flatten_live_requires_all_account_bound_gates() {
        let policy = parse_okx_flatten_launch_policy(
            &args(&[
                "--live",
                "--expected-account-id=account-a",
                "--live-confirmation=OKX-LIVE-FLATTEN:account-a",
            ]),
            OKXLaunchEnvironment::default(),
        )
        .unwrap();

        assert_eq!(policy.environment, OKXEnvironment::Live);
        assert_eq!(policy.expected_account_id.as_deref(), Some("account-a"));
    }

    #[rstest]
    fn execution_unset_defaults_to_demo_dry_run() {
        let policy =
            parse_okx_execution_launch_policy(&[], OKXLaunchEnvironment::default()).unwrap();

        assert_eq!(policy.environment, OKXEnvironment::Demo);
        assert!(!policy.orders_enabled);
    }

    #[rstest]
    fn execution_demo_orders_require_explicit_gate() {
        let policy = parse_okx_execution_launch_policy(
            &args(&["--demo-orders"]),
            OKXLaunchEnvironment::default(),
        )
        .unwrap();

        assert_eq!(policy.environment, OKXEnvironment::Demo);
        assert!(policy.orders_enabled);
    }

    #[rstest]
    fn execution_live_connection_remains_dry_run_without_order_gate() {
        let policy =
            parse_okx_execution_launch_policy(&args(&["--live"]), OKXLaunchEnvironment::default())
                .unwrap();

        assert_eq!(policy.environment, OKXEnvironment::Live);
        assert!(!policy.orders_enabled);
    }

    #[rstest]
    fn execution_live_orders_without_live_connection_are_rejected() {
        let error = parse_okx_execution_launch_policy(
            &args(&["--live-orders"]),
            OKXLaunchEnvironment::default(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("--live connection gate"));
    }

    #[rstest]
    fn execution_live_orders_without_confirmation_fail_closed() {
        let error = parse_okx_execution_launch_policy(
            &args(&["--live", "--live-orders"]),
            OKXLaunchEnvironment::default(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("expected-account-id"));
    }

    #[rstest]
    fn execution_live_confirmation_for_wrong_account_is_rejected() {
        let error = parse_okx_execution_launch_policy(
            &args(&[
                "--live",
                "--live-orders",
                "--expected-account-id=account-b",
                "--live-confirmation=OKX-LIVE-ORDERS:account-b",
            ]),
            OKXLaunchEnvironment {
                configured_account_id: Some("account-a"),
                ..Default::default()
            },
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("does not match configured account")
        );
    }

    #[rstest]
    fn execution_live_orders_require_all_account_bound_gates() {
        let policy = parse_okx_execution_launch_policy(
            &args(&[
                "--live",
                "--live-orders",
                "--expected-account-id=account-a",
                "--live-confirmation=OKX-LIVE-ORDERS:account-a",
            ]),
            OKXLaunchEnvironment::default(),
        )
        .unwrap();

        assert_eq!(policy.environment, OKXEnvironment::Live);
        assert!(policy.orders_enabled);
        assert_eq!(policy.expected_account_id.as_deref(), Some("account-a"));
    }

    #[rstest]
    fn invalid_boolean_environment_switch_fails_closed() {
        let error = parse_okx_execution_launch_policy(
            &[],
            OKXLaunchEnvironment {
                live: Some("yes"),
                ..Default::default()
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("exactly true or false"));
    }
}
