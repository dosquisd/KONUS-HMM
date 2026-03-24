import warnings
from enum import Enum
from typing import Any, Callable, Union

import numpy as np
import numpy.typing as npt

from .constants import MIN_VALUE_THRESHOLD

CustomScale = Union[int, float, None, Callable[[npt.NDArray], float | np.floating]]


class Normalizer(Enum):
    CUSTOM = "custom"
    MIN_MAX = "min_max"
    Z_SCORE = "z_score"
    ROBUST = "robust"
    DECIMAL_SCALING = "decimal_scaling"
    NONE = "none"

    def __custom(
        self,
        data: npt.NDArray[np.floating],
        /,
        scale: CustomScale,
        **_,
    ) -> npt.NDArray[np.floating]:
        """Applies custom normalization to the data."""

        # Determine the scale value based on the provided scale parameter
        if scale is None:
            scale = np.max(np.abs(data), axis=0)
        elif callable(scale):
            scale = scale(data)  # type: ignore
        elif isinstance(scale, (int, float)):
            shape = (data.shape[1],) if data.ndim > 1 else ()
            scale = np.full(shape, scale)  # type: ignore
            # Or just leave it as is, since numpy will broadcast it correctly during division
        else:
            raise ValueError(
                "Scale must be a int, float, None, or a callable that takes the data as input."
            )

        normalized_data = data / scale  # type: ignore
        return normalized_data

    def __min_max_normalize(
        self, data: npt.NDArray[np.floating]
    ) -> npt.NDArray[np.floating]:
        """Applies Min-Max normalization to the data."""
        min_val = np.min(data, axis=0)
        max_val = np.max(data, axis=0)
        normalized_data = (data - min_val) / (max_val - min_val)
        return normalized_data

    def __z_score_normalize(
        self, data: npt.NDArray[np.floating]
    ) -> npt.NDArray[np.floating]:
        """Applies Z-score normalization to the data."""
        mean = np.mean(data, axis=0)
        std = np.std(data, axis=0)
        normalized_data = (data - mean) / std
        return normalized_data

    def __robust_normalize(
        self, data: npt.NDArray[np.floating]
    ) -> npt.NDArray[np.floating]:
        """Applies Robust normalization to the data."""
        median = np.median(data, axis=0)
        q1 = np.percentile(data, 25, axis=0)
        q3 = np.percentile(data, 75, axis=0)
        iqr = q3 - q1
        iqr[np.abs(iqr) < MIN_VALUE_THRESHOLD] = MIN_VALUE_THRESHOLD

        normalized_data = (data - median) / iqr
        return normalized_data

    def __decimal_scaling_normalize(
        self, data: npt.NDArray[np.floating]
    ) -> npt.NDArray[np.floating]:
        """Applies Decimal Scaling normalization to the data."""
        max_abs = np.max(np.abs(data), axis=0)
        j = np.ceil(np.log10(max_abs + 1))
        normalized_data = data / (10**j)
        return normalized_data

    def normalize(
        self, data: npt.NDArray[np.floating], /, **kwargs: Any
    ) -> npt.NDArray[np.floating]:
        match self:
            case Normalizer.CUSTOM:
                return self.__custom(data, **kwargs)
            case Normalizer.MIN_MAX:
                return self.__min_max_normalize(data)
            case Normalizer.Z_SCORE:
                return self.__z_score_normalize(data)
            case Normalizer.ROBUST:
                return self.__robust_normalize(data)
            case Normalizer.DECIMAL_SCALING:
                return self.__decimal_scaling_normalize(data)
            case Normalizer.NONE:
                return data
            case _:
                warnings.warn(
                    f"Unsupported normalization method: {self.value}. Returning original data."
                )
                return data
