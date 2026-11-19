import os
import tempfile

# Keep test trades out of the real trades.jsonl that the dashboard reads.
# Must run before bot.trade_ledger is imported (it reads the path at import).
os.environ.setdefault(
    "TRADE_LEDGER_PATH",
    os.path.join(tempfile.mkdtemp(prefix="nh-tests-"), "trades.jsonl"),
)
