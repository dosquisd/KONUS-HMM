import matplotlib.pyplot as plt
import scienceplots  # noqa: F401

from utils import dtypes
from utils.constants import DATADIR, FIGURESDIR, MIN_VALUE_THRESHOLD, ROOTDIR
from utils.load import load_data_per_event, load_events
from utils.normalizer import Normalizer


def setup_scienceplots() -> None:
    plt.style.use(["science", "ieee"])


__all__ = [
    "MIN_VALUE_THRESHOLD",
    "DATADIR",
    "FIGURESDIR",
    "ROOTDIR",
    "Normalizer",
    "dtypes",
    "load_data_per_event",
    "load_events",
    "setup_scienceplots",
]
