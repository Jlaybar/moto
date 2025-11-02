import numpy as np
from scipy.optimize import curve_fit
from typing import List, Dict, Tuple


def exp_func(x: np.ndarray, a: float, b: float, c: float) -> np.ndarray:
    return a * np.exp(b * x) + c


def _compute_exponential_fit(kms: np.ndarray, precios: np.ndarray) -> Tuple[np.ndarray, np.ndarray, Tuple[float, float, float], str]:
    fit_x = None
    fit_y = None
    popt = None
    eq_text = ''
    if kms.size >= 3:
        try:
            p0 = [float(np.max(precios)), -1e-4, float(np.min(precios))]
            popt, _ = curve_fit(exp_func, kms, precios, p0=p0, maxfev=5000)
            x_line = np.linspace(float(np.min(kms)) - 100.0, float(np.max(kms)) + 100.0, 200)
            fit_x = x_line
            fit_y = exp_func(x_line, *popt)
            eq_text = f'y = {popt[0]:.0f} * e^({popt[1]:.7f} * x) + {popt[2]:.0f}'
        except Exception as e:
            eq_text = f'Ajuste no disponible: {e}'
    else:
        eq_text = 'Datos insuficientes para ajuste'
    return fit_x, fit_y, popt, eq_text


def _classify_points(kms: np.ndarray, precios: np.ndarray, years: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    # x features
    x_log = np.log1p(kms).astype(float)
    yrs_num = np.array([np.nan if (y is None or (isinstance(y, float) and np.isnan(y))) else float(y) for y in years])
    mask_fit = ~np.isnan(yrs_num)

    # default outputs
    residuals = np.zeros_like(precios, dtype=float)
    z_scores = np.zeros_like(precios, dtype=float)
    y_pred = np.zeros_like(precios, dtype=float)
    tags = np.array(['sin_anno' if (y is None or (isinstance(y, float) and np.isnan(y))) else 'normal' for y in years], dtype=object)

    if np.sum(mask_fit) >= 3:
        X = np.c_[np.ones(np.sum(mask_fit)), x_log[mask_fit], yrs_num[mask_fit]]
        yv = precios[mask_fit]
        try:
            beta, *_ = np.linalg.lstsq(X, yv, rcond=None)
            yr_fill = float(np.nanmedian(yrs_num[mask_fit]))
            Xall = np.c_[np.ones(len(kms)), x_log, np.where(np.isnan(yrs_num), yr_fill, yrs_num)]
            y_pred = Xall @ beta
            residuals = precios - y_pred
            mad = float(np.median(np.abs(residuals - np.median(residuals))))
            sigma = (1.4826 * mad) if mad > 0 else float(np.std(residuals))
            if sigma <= 0:
                sigma = 1.0
            z_scores = residuals / sigma
            new_tags = []
            for yval, zz, rr in zip(years, z_scores, residuals):
                if (yval is None) or (isinstance(yval, float) and np.isnan(yval)):
                    new_tags.append('sin_anno')
                elif (zz <= -1.5) and (rr <= -1000):
                    new_tags.append('chollo')
                elif (zz >= 1.5) and (rr >= 1000):
                    new_tags.append('caro')
                else:
                    new_tags.append('normal')
            tags = np.array(new_tags, dtype=object)
        except Exception:
            pass

    return residuals, z_scores, y_pred, tags


def _year_weight(years: np.ndarray) -> np.ndarray:
    # Simple weighting based on recency: normalize into [0.5, 1.5]
    yrs = np.array([np.nan if (y is None or (isinstance(y, float) and np.isnan(y))) else float(y) for y in years])
    if np.all(np.isnan(yrs)):
        return np.ones_like(yrs)
    mn = np.nanmin(yrs)
    mx = np.nanmax(yrs)
    if not np.isfinite(mn) or not np.isfinite(mx) or mn == mx:
        return np.ones_like(yrs)
    norm = (yrs - mn) / (mx - mn)
    return 0.5 + norm  # [0.5, 1.5]


def modelfit(items_json: List[Dict], marca: str, modelo: str) -> List[Dict]:
    # Extract arrays
    kms, precios, years = [], [], []
    for it in items_json:
        try:
            km_val = it.get('km', None)
            pr_val = it.get('price', None)
            yr_val = it.get('year', None)
            if km_val is None or pr_val is None:
                continue
            kms.append(int(km_val))
            precios.append(float(pr_val))
            try:
                y = int(str(yr_val).strip()) if yr_val is not None and str(yr_val).strip() != '' else None
                if y is not None and (y < 1970 or y > 2050):
                    y = None
            except Exception:
                y = None
            years.append(y)
        except Exception:
            continue

    if len(kms) == 0:
        return []

    kms_arr = np.asarray(kms, dtype=float)
    precios_arr = np.asarray(precios, dtype=float)
    years_arr = np.asarray(years, dtype=object)

    # Model computations
    fit_x, fit_y, popt, eq_text = _compute_exponential_fit(kms_arr, precios_arr)
    residuals, z_scores, y_pred, tags = _classify_points(kms_arr, precios_arr, years_arr)
    w_year = _year_weight(years_arr)

    # Attach results back to items in the same order they were read (filtered only to valid km/price)
    # Build a new list preserving the subset used
    out: List[Dict] = []
    idx = 0
    for it in items_json:
        km_val = it.get('km', None)
        pr_val = it.get('price', None)
        if km_val is None or pr_val is None:
            # Skip items not used in model
            continue
        rec = dict(it)
        rec['tag'] = str(tags[idx]) if idx < len(tags) else 'normal'
        rec['residual'] = float(residuals[idx]) if idx < len(residuals) else 0.0
        rec['z'] = float(z_scores[idx]) if idx < len(z_scores) else 0.0
        rec['y_pred'] = float(y_pred[idx]) if idx < len(y_pred) else float(pr_val)
        rec['year_weight'] = float(w_year[idx]) if idx < len(w_year) and np.isfinite(w_year[idx]) else 1.0
        # add global fit info (duplicate for simplicity)
        rec['fit_x'] = (fit_x.tolist() if isinstance(fit_x, np.ndarray) else None)
        rec['fit_y'] = (fit_y.tolist() if isinstance(fit_y, np.ndarray) else None)
        rec['eq_text'] = eq_text
        if popt is not None:
            rec['fit_params'] = {'a': float(popt[0]), 'b': float(popt[1]), 'c': float(popt[2])}
        else:
            rec['fit_params'] = None
        out.append(rec)
        idx += 1

    return out

