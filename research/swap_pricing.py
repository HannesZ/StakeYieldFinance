
import numpy as np
import pandas as pd

def bootstrap_zeros_from_swaps(
    maturities_years,
    par_swap_rates,
    payment_frequency=2,          # 1=annual, 2=semiannual, 4=quarterly, etc.
    compounding="cont",           # "cont" or "annual"
):
    """
    Bootstrap discount factors and zero-coupon (spot) rates from par swap rates.

    Parameters
    ----------
    maturities_years : array-like
        Increasing swap maturities in years, e.g. [0.5, 1, 2, 3, 5, 7, 10].
        Each must be an integer multiple of 1/payment_frequency.
    par_swap_rates : array-like
        Par swap rates as decimals (e.g., 0.0325 for 3.25%) for the corresponding maturities.
    payment_frequency : int
        Fixed-leg payment frequency per year (1, 2, 4, 12...).
    compounding : {"cont","annual"}
        Convention for reporting zero rates. Discount factors are convention-free.

    Returns
    -------
    pandas.DataFrame with columns:
        - maturity: years
        - discount_factor: P(0,T)
        - zero_rate: spot rate to T (per 'compounding')
    """
    maturities_years = np.asarray(maturities_years, dtype=float)
    par_swap_rates = np.asarray(par_swap_rates, dtype=float)

    if maturities_years.ndim != 1 or par_swap_rates.ndim != 1:
        raise ValueError("Inputs must be 1-D arrays.")
    if len(maturities_years) != len(par_swap_rates):
        raise ValueError("maturities_years and par_swap_rates must have the same length.")
    if not np.all(np.diff(maturities_years) > 0):
        raise ValueError("maturities_years must be strictly increasing.")
    if payment_frequency <= 0 or int(payment_frequency) != payment_frequency:
        raise ValueError("payment_frequency must be a positive integer.")
    if compounding not in ("cont", "annual"):
        raise ValueError("compounding must be 'cont' or 'annual'.")

    alpha = 1.0 / payment_frequency  # accrual per period
    # Build a uniform payment grid up to the max maturity
    max_T = maturities_years[-1]
    grid = np.round(np.arange(alpha, max_T + 1e-12, alpha), 12)  # payment dates (years)

    # Map each swap maturity to its index in the payment grid
    def _grid_index(T):
        # Ensure T sits exactly on the grid (within numerical tolerance)
        idx = np.where(np.isclose(grid, T, atol=1e-10))[0]
        if len(idx) == 0:
            raise ValueError(
                f"Maturity {T} is not aligned to payment grid of step {alpha}."
            )
        return int(idx[0])

    # Storage for discount factors at each grid payment date
    P_grid = np.empty_like(grid)
    P_grid[:] = np.nan

    # Bootstrap each swap maturity in ascending order
    for T, S in zip(maturities_years, par_swap_rates):
        N = _grid_index(T) + 1  # number of coupons to this maturity
        # Sum of known DF * accrual up to the penultimate coupon
        if N == 1:
            sum_known = 0.0
        else:
            if np.any(np.isnan(P_grid[: N - 1])):
                # This ensures maturities are nested: each later swap relies on earlier DFs.
                raise RuntimeError(
                    f"Missing discount factors before solving T={T}. "
                    "Make sure maturities are nested on the chosen frequency."
                )
            sum_known = alpha * np.sum(P_grid[: N - 1])

        # Solve for the last (unknown) DF using the par condition:
        # S * (alpha * sum_{i=1..N-1} P_i + alpha * P_N) = 1 - P_N
        # => P_N = (1 - S * alpha * sum_known/alpha) / (1 + S * alpha)
        # but sum_known already includes alpha, so:
        P_N = (1.0 - S * sum_known) / (1.0 + S * alpha)
        if P_N <= 0 or P_N >= 1.2:
            raise RuntimeError(
                f"Infeasible DF {P_N:.6f} at T={T}. "
                "Check inputs: rates, frequency, and monotonicity."
            )
        P_grid[N - 1] = P_N

    # Build outputs only at the requested maturities (not every grid point)
    out_rows = []
    for T in maturities_years:
        N = _grid_index(T)  # zero-based index
        P = P_grid[N]
        if compounding == "cont":
            r = -np.log(P) / T
        else:  # annual comp
            r = P ** (-1.0 / T) - 1.0
        out_rows.append((T, P, r))

    df = pd.DataFrame(out_rows, columns=["maturity", "discount_factor", "zero_rate"])
    return df

# --- Example usage ---
if __name__ == "__main__":
    # Example: semiannual pay swaps (2x per year)
    maturities = [1.0, 2.0, 3.0, 4.0]      # years
    par_rates  = [0.05, 0.0511, 0.053, 0.0547]  # 3.5%, 3.7%, ...
    table = bootstrap_zeros_from_swaps(maturities, par_rates, payment_frequency=1, compounding="annual")
    print(table)
