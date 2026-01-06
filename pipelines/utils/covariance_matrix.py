import numpy as np
import polars as pl


def get_factor_loadings_matrix(
    tickers: list[str], factor_loadings: pl.DataFrame
) -> np.ndarray:
    return (
        factor_loadings.filter(pl.col("ticker").is_in(tickers))
        .sort("ticker", "factor")
        .pivot(index="ticker", on="factor", values="loading")
        .drop("ticker")
        .to_numpy()
    )


def get_factor_covariance_matrix(factor_covariances: pl.DataFrame) -> np.ndarray:
    return (
        factor_covariances.sort("factor_1", "factor_2")
        .pivot(index="factor_1", on="factor_2", values="covariance")
        .drop("factor_1")
        .to_numpy()
    )


def get_idio_vol_matrix(tickers: list[str], idio_vol: pl.DataFrame) -> np.ndarray:
    return np.diag(
        idio_vol.filter(pl.col("ticker").is_in(tickers))
        .sort("ticker")["idio_vol"]
        .to_numpy()
    )


def construct_covariance_matrix(
    tickers: list[str],
    factor_loadings_matrix: np.ndarray,
    factor_covariance_matrix: np.ndarray,
    idio_vol_matrix: np.ndarray,
) -> pl.DataFrame:
    covariance_matrix_np = (
        factor_loadings_matrix @ factor_covariance_matrix @ factor_loadings_matrix.T
        + idio_vol_matrix**2
    )

    covariance_matrix = pl.from_numpy(covariance_matrix_np)
    covariance_matrix.columns = tickers
    covariance_matrix = covariance_matrix.select(
        pl.Series(tickers).alias("ticker"), *tickers
    )

    return covariance_matrix


def get_covariance_matrix(
    tickers: list[str],
    factor_loadings: pl.DataFrame,
    factor_covariances: pl.DataFrame,
    idio_vol: pl.DataFrame,
):
    factor_loadings_matrix = get_factor_loadings_matrix(tickers, factor_loadings)
    factor_covariance_matrix = get_factor_covariance_matrix(factor_covariances)
    idio_vol_matrix = get_idio_vol_matrix(tickers, idio_vol)

    covariance_matrix = construct_covariance_matrix(
        tickers, factor_loadings_matrix, factor_covariance_matrix, idio_vol_matrix
    )

    return covariance_matrix
