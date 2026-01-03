import polars as pl
import cvxpy as cp
import numpy as np


def solve_quadratic_problem(
    n_assets: int,
    alphas: np.ndarray,
    covariance_matrix: np.ndarray,
    lambda_: float,
):
    weights = cp.Variable(n_assets)

    objective = cp.Maximize(
        cp.matmul(weights, alphas)
        - 0.5 * lambda_ * cp.quad_form(weights, covariance_matrix)
    )

    constraints = [
        cp.sum(weights) == 1,  # Full investment
        weights >= 0,  # Long only
    ]

    problem = cp.Problem(objective, constraints)
    problem.solve()

    return weights.value


def get_optimal_weights(
    alphas: pl.DataFrame,
    covariance_matrix: pl.DataFrame,
    lambda_: float,
) -> pl.DataFrame:
    tickers = alphas["ticker"].sort().to_list()

    optimal_weights = solve_quadratic_problem(
        n_assets=len(tickers),
        alphas=alphas["alpha"].to_numpy(),
        covariance_matrix=covariance_matrix.drop("ticker").to_numpy(),
        lambda_=lambda_,
    )

    return pl.DataFrame({"ticker": tickers, "weight": optimal_weights})


def predict_lambda(data: list[tuple[float]], active_risk: float) -> float:
    def fit_model(data: np.ndarray) -> float:
        lambda_ = data[:, 0]
        sigma = data[:, 1]

        X = 1 / (2 * lambda_)

        M = np.dot(X, sigma) / np.dot(X, X)

        return M

    data = np.array(data)

    M = fit_model(data)

    return M / (2 * active_risk)


def get_active_weights(
    weights: pl.DataFrame, benchmark_weights: pl.DataFrame
) -> pl.DataFrame:
    return (
        weights.join(
            other=benchmark_weights.rename({"weight": "benchmark_weight"}).drop("date"),
            on=["ticker"],
            how="left",
        )
        .with_columns(
            pl.col("weight").sub(pl.col("benchmark_weight")).alias("active_weight")
        )
        .select("ticker", "active_weight")
    )


def get_active_risk(
    active_weights: pl.DataFrame, covariance_matrix: pl.DataFrame
) -> float:
    active_weights = active_weights.sort("ticker")["active_weight"].to_numpy()
    covariance_matrix = covariance_matrix.drop("ticker").to_numpy()

    return np.sqrt(active_weights @ covariance_matrix @ active_weights.T) * np.sqrt(252)


def get_optimal_weights_dynamic(
    alphas: pl.DataFrame,
    covariance_matrix: pl.DataFrame,
    benchmark_weights: pl.DataFrame,
    target_active_risk: float = 0.05,
) -> tuple[pl.DataFrame, float, float]:
    active_risk = float("inf")
    lambda_ = None
    error = 0.005
    max_iterations = 5
    iterations = 1
    data = []

    while abs(active_risk - target_active_risk) > error:
        if lambda_ is None:
            lambda_ = 100
        else:
            lambda_ = predict_lambda(data, target_active_risk)

        optimal_weights = get_optimal_weights(alphas, covariance_matrix, lambda_)

        active_weights = get_active_weights(optimal_weights, benchmark_weights)
        active_risk = get_active_risk(active_weights, covariance_matrix)

        data.append((lambda_, active_risk))

        if iterations >= max_iterations:
            break
        else:
            iterations += 1

    return optimal_weights, lambda_, active_risk
