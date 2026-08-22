from zoneinfo import ZoneInfo

FACTORS = sorted(["MTUM", "QUAL", "USMV", "VLUE", "SPY"])
WINDOW = 252
DISABLE_TQDM = False
IC = 0.05
TIME_ZONE = ZoneInfo("UTC")
TARGET_ACTIVE_RISK = 0.05

# Signal lookbacks, in trading days (~21 per month)
REVERSAL_WINDOW = 21  # 1-month reversal
MOMENTUM_WINDOW = 231  # 11-month rolling return
MOMENTUM_LAG = 21  # lagged 1 month -> 12-1 momentum

# Composite alpha: 50/50 blend of reversal and momentum
COMPOSITE_WEIGHTS = {"reversal": 0.5, "momentum": 0.5}

# Signal the portfolio optimizer trades on
PRODUCTION_SIGNAL = "composite"
