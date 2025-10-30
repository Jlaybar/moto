import numpy as np
import pandas as pd
from scipy.optimize import curve_fit

from bokeh.io import output_notebook, show
from bokeh.resources import INLINE
from bokeh.plotting import figure
from bokeh.models import (
    ColumnDataSource, HoverTool, TapTool, OpenURL, NumeralTickFormatter, Label, ColorBar,
    Select, CustomJS, CDSView, BooleanFilter, Band, Title
)
from bokeh.transform import linear_cmap, factor_cmap
from bokeh.palettes import Blues256, Pastel1, Pastel2
from bokeh.layouts import column

from db_sqlite3_api  import db_read_dict



# --------------------------------------------------------------------------------------------------------------------------------------------------------

def plot_price_km_by_year_json(result,marca,modelo):
    """
    Grafica interactiva (Bokeh) Precio vs KilÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³metros para un modelo.
    - Fondo blanco, textos/etiquetas en negro; grid gris suave.
    - Puntos rellenos en degradado azul por aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o (mÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡s oscuro = mÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡s antiguo).
    - Borde azul oscuro; verde/rojo si es chollo/caro.
    - Barra de color AÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o.
    - Selector: AÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o (filtra).
    - Hover: Precio, Km, AÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o, URL; Tap: abre URL.
    """
    import math, re
    # Mapa provinciaId -> nombre
    try:
        from catalog.dict_prov import dict_prov
    except Exception:
        dict_prov = {}

    # Asegura que BokehJS se cargue en el contexto del notebook
    try:
        output_notebook(INLINE)
    except Exception:
        pass

    # Preparar DataFrame y filtrar por modelo
    df = pd.DataFrame(result)
    registros = df.to_dict(orient='records')

     # === Lectura directa de campos: price, km, url, year ===
    def _coerce_year(y):
        try:
            y = int(str(y).strip())
            if 1980 <= y <= 2035:
                return y
        except Exception:
            pass
        return None

    kms, precios, urls, years = [], [], [], []
    prov_ids, prov_names = [], []
    for it in registros:
        precio = it.get('price', None)
        km_val = it.get('km', None)
        url = it.get('url', None)
        year_val = _coerce_year(it.get('year', None))

        if precio is None or (isinstance(precio, float) and math.isnan(precio)):
            continue
        if km_val is None or (isinstance(km_val, float) and math.isnan(km_val)):
            continue

        kms.append(int(km_val))
        precios.append(float(precio))
        urls.append(url if isinstance(url, str) and url.strip() else 'javascript:void(0)')
        years.append(year_val)
        prov_raw = it.get('provinceId', None)
        try:
            prov_int = int(prov_raw) if prov_raw is not None and str(prov_raw).strip() != '' else None
        except Exception:
            prov_int = None
        prov_name = dict_prov.get(prov_int, (str(prov_raw) if prov_raw is not None else '-'))
        prov_ids.append(prov_int)
        prov_names.append(prov_name)

    if len(kms) == 0:
        print('Sin datos vÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡lidos para graficar')
        return

    # === Ordenar por km para lÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â­nea de ajuste (sin cambios) ===
    order = np.argsort(kms)
    kms_arr = np.array(kms)[order]
    precios_arr = np.array(precios)[order]
    urls_arr = np.array(urls, dtype=object)[order]
    years_arr = np.array(years, dtype=object)[order]
    prov_ids_arr = np.array(prov_ids, dtype=object)[order]
    prov_names_arr = np.array(prov_names, dtype=object)[order]

    # === Ajuste exponencial (igual) ===
    def exp_func(x, a, b, c):
        return a * np.exp(b * x) + c

    fit_x, fit_y = None, None
    if len(kms_arr) >= 3:
        try:
            p0 = [max(precios_arr), -1e-4, min(precios_arr)]
            popt, _ = curve_fit(exp_func, kms_arr, precios_arr, p0=p0, maxfev=5000)
            x_line = np.linspace(float(kms_arr.min()) - 100, float(kms_arr.max()) + 100, 200)
            fit_x = x_line
            fit_y = exp_func(x_line, *popt)
            eq_text = f'y = {popt[0]:.0f}ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â·e^({popt[1]:.7f}ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â·x) + {popt[2]:.0f}'
        except Exception as e:
            eq_text = f'Ajuste no disponible: {e}'
    else:
        eq_text = 'Datos insuficientes para ajuste'

    # === ClasificaciÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³n chollo/caro (igual) ===
    x_log = np.log1p(kms_arr).astype(float)
    yrs_num = np.array([np.nan if y is None else float(y) for y in years_arr])
    mask_fit = ~np.isnan(yrs_num)
    if np.sum(mask_fit) >= 3:
        X = np.c_[np.ones(np.sum(mask_fit)), x_log[mask_fit], yrs_num[mask_fit]]
        yv = precios_arr[mask_fit]
        try:
            beta, *_ = np.linalg.lstsq(X, yv, rcond=None)
        except Exception:
            beta = np.array([np.median(yv), 0.0, 0.0])
        yr_fill = np.nanmedian(yrs_num[mask_fit])
        Xall = np.c_[np.ones(len(kms_arr)), x_log, np.where(np.isnan(yrs_num), yr_fill, yrs_num)]
        y_pred = Xall @ beta
        residuals = precios_arr - y_pred
        z_scores = np.zeros_like(residuals, dtype=float)
        unique_years = sorted(set(int(y) for y in yrs_num[mask_fit]))
        global_median = float(np.median(residuals[mask_fit])) if np.any(mask_fit) else 0.0
        global_mad = float(np.median(np.abs(residuals[mask_fit] - global_median))) or 1.0
        for yr in unique_years:
            idx = (yrs_num == yr)
            r = residuals[idx]
            if r.size >= 8:
                med = float(np.median(r))
                mad = float(np.median(np.abs(r - med))) or global_mad
                z_scores[idx] = (r - med) / (1.4826 * mad)
            else:
                z_scores[idx] = (r - global_median) / (1.4826 * global_mad)
        tags = []
        for rr, zz, yv in zip(residuals, z_scores, yrs_num):
            if np.isnan(yv):
                tags.append('sin_aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o')
            elif (zz <= -1.5) and (rr <= -1000):
                tags.append('chollo')
            elif (zz >= 1.5) and (rr >= 1000):
                tags.append('caro')
            else:
                tags.append('normal')
    else:
        residuals = np.zeros_like(precios_arr, dtype=float)
        z_scores = np.zeros_like(precios_arr, dtype=float)
        tags = ['sin_aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o' if (y is None) else 'normal' for y in years_arr]

    # === ConstrucciÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³n de fuentes (igual) ===
    mask_year = np.array([y is not None for y in years_arr])

    def _fmt_eur(v): return f'{v:,.0f}ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬'.replace(',', '.')
    def _fmt_km(v): return f'{v:,.0f} km'.replace(',', '.')

    dark_blue = '#003366'
    green = '#2ECC71'
    red = '#E74C3C'
    border_colors = []
    for t in tags:
        if t == 'chollo': border_colors.append(green)
        elif t == 'caro': border_colors.append(red)
        else: border_colors.append(dark_blue)
    border_colors = np.array(border_colors, dtype=object)

    data_with_year = dict(
        km=kms_arr[mask_year],
        precio=precios_arr[mask_year],
        url=urls_arr[mask_year],
        year=[int(v) for v in years_arr[mask_year]],
        precio_fmt=[_fmt_eur(v) for v in precios_arr[mask_year]],
        km_fmt=[_fmt_km(v) for v in kms_arr[mask_year]],
        year_fmt=[str(int(v)) for v in years_arr[mask_year]],
        residual=residuals[mask_year],
        z=z_scores[mask_year],
        tag=[t for (t,m) in zip(tags, mask_year) if m],
        border_color=border_colors[mask_year],
        prov=[str(v) for v in prov_names_arr[mask_year]],
    )
    data_no_year = dict(
        km=kms_arr[~mask_year],
        precio=precios_arr[~mask_year],
        url=urls_arr[~mask_year],
        year=[None]*int((~mask_year).sum()),
        precio_fmt=[_fmt_eur(v) for v in precios_arr[~mask_year]],
        km_fmt=[_fmt_km(v) for v in kms_arr[~mask_year]],
        year_fmt=['-']*int((~mask_year).sum()),
        prov=[str(v) for v in prov_names_arr[~mask_year]],
    )

    source_y = ColumnDataSource(data_with_year)
    source_n = ColumnDataSource(data_no_year)

    black = '#000000'
    blue = '#1E90FF'
    bright_blue = '#00BFFF'
    white = '#FFFFFF'
    gray = '#DDDDDD'

    # === TÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â­tulos (igual) ===
    p = figure(width=900, height=600, background_fill_color=white, toolbar_location='above')
    p.title = None
    p.add_layout(Title(text='RelaciÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³n Precio vs KilÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³metros', text_font_size='16pt', text_color=black), 'above')
    p.add_layout(Title(text=marca+":"+modelo, text_font_style='bold', text_font_size='20pt', text_color=bright_blue), 'above')

    # === EstÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â©tica (igual) ===
    p.min_border_top = 90
    p.axis.axis_line_color = black
    p.axis.major_tick_line_color = black
    p.axis.minor_tick_line_color = black
    p.axis.major_label_text_color = black
    p.xaxis.axis_label_text_color = black
    p.yaxis.axis_label_text_color = black
    p.grid.grid_line_color = gray
    p.grid.grid_line_alpha = 0.4
    p.xaxis.axis_label = 'KilÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³metros Recorridos (km)'
    p.yaxis.axis_label = 'Precio (ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬)'
    p.yaxis.formatter = NumeralTickFormatter(format='0,0')

    r_y_norm = r_y_chol = r_y_caro = None
    if len(data_with_year['km']) > 0:
        y_low = float(np.min(data_with_year['prov']))
        y_high = float(np.max(data_with_year['prov']))
        mapper = linear_cmap(field_name='year', palette=list(reversed(Blues256)), low=y_low, high=y_high)
        filt_y = BooleanFilter(booleans=[True]*len(data_with_year['km']))

        mask_tag_normal = [t == 'normal' for t in data_with_year['tag']]
        mask_tag_chollo = [t == 'chollo' for t in data_with_year['tag']]
        mask_tag_caro = [t == 'caro' for t in data_with_year['tag']]
        filt_tag_normal = BooleanFilter(booleans=mask_tag_normal)
        filt_tag_chollo = BooleanFilter(booleans=mask_tag_chollo)
        filt_tag_caro = BooleanFilter(booleans=mask_tag_caro)

        view_norm = CDSView(source=source_y, filters=[filt_y, filt_tag_normal])
        view_chol = CDSView(source=source_y, filters=[filt_y, filt_tag_chollo])
        view_caro = CDSView(source=source_y, filters=[filt_y, filt_tag_caro])

        r_y_norm = p.circle('km', 'precio', size=10, color=mapper, line_color=dark_blue, line_width=1.5, alpha=0.95, source=source_y, view=view_norm)
        r_y_chol = p.circle('km', 'precio', size=10, fill_color=green, line_color='#006400', line_width=1.8, alpha=0.95, source=source_y, view=view_chol, legend_label='Chollo')
        r_y_caro = p.circle('km', 'precio', size=10, fill_color=red, line_color='#800000', line_width=1.8, alpha=0.95, source=source_y, view=view_caro, legend_label='Timo')

        p.legend.location = 'top_right'
        p.legend.background_fill_color = 'white'
        p.legend.background_fill_alpha = 0.8
        p.legend.label_text_color = black
        p.legend.border_line_color = black
        p.legend.border_line_alpha = 0.3

        cbar = ColorBar(color_mapper=mapper['transform'], title='AÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o', label_standoff=8)
        p.add_layout(cbar, 'right')

    r_n = None
    if len(data_no_year['km']) > 0:
        filt_prov_n = BooleanFilter(booleans=[True]*len(data_no_year['km']))
        view_n = CDSView(source=source_n, filters=[filt_prov_n])
        r_n = p.circle('km', 'precio', size=9, color=black, line_color=dark_blue, line_width=1.5, alpha=0.8, source=source_n, view=view_n)

    if fit_x is not None and fit_y is not None:
        try:
            y_fit_pts = exp_func(kms_arr, *popt)
            resid = precios_arr - y_fit_pts
            mad = float(np.median(np.abs(resid - np.median(resid))))
            sigma = (1.4826*mad) if mad > 0 else float(np.std(resid))
            low = fit_y - 1.96*sigma
            high = fit_y + 1.96*sigma
            band_src = ColumnDataSource(dict(x=fit_x, low=low, high=high))
            band = Band(base='x', lower='low', upper='high', source=band_src, level='underlay', fill_color='#BBBBBB', fill_alpha=0.18, line_color=None)
            p.add_layout(band)
        except Exception:
            pass
        p.line(fit_x, fit_y, line_width=3, color=blue)

    renderers_for_hover = [r for r in (r_y_norm, r_y_chol, r_y_caro, r_n) if r is not None]
    hover = HoverTool(renderers=renderers_for_hover, tooltips=[
        ('AÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o', '@year_fmt'),
        ('KilÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³metros', '@km_fmt'),
        ('Precio', '@precio_fmt'),
        ('URL', '@url')
    ])
    p.add_tools(hover)
    try:
        hover.tooltips = list(hover.tooltips) + [('Provincia', '@prov')]
    except Exception:
        pass
    tap = TapTool()
    p.add_tools(tap)
    tap.callback = OpenURL(url='@url')

    if eq_text:
        label = Label(x=10, y=p.height - 10, x_units='screen', y_units='screen',
                      text=eq_text, text_color=black, text_font='courier', text_font_size='10pt',
                      background_fill_color=white, background_fill_alpha=0.8)
    p.add_layout(label)

    controls = []
    if 'year' in data_with_year and len(data_with_year['prov']) > 0:
        years_unique = sorted({int(y) for y in data_with_year['prov'] if y is not None})
        sel_year = Select(title='Filtrar por aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o', value='Todos',
                          options=(['Todos'] + [str(y) for y in years_unique] + (['Sin aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o'] if r_n is not None else [])))
        cb_year = CustomJS(args=dict(sel=sel_year, src_y=source_y, src_n=source_n, filt_prov_n=filt_prov_n,  src_n=source_n, filt_prov_n=filt_prov_n,  filt_prov_y=filt_prov_y if len(data_with_year['km'])>0 else None,
                                     r_n=r_n, r_y_norm=r_y_norm, r_y_chol=r_y_chol, r_y_caro=r_y_caro), code="""
const val = sel.value;
if (!src_y || !src_y.data || !src_y.data['prov']) return;
const years = src_y.data['prov'];
const n = provs.length;
let arr = new Array(n).fill(true);
if (val === 'Todos') {
  arr = Array(n).fill(true);
  if (r_y_norm) r_y_norm.visible = true;
  if (r_y_chol) r_y_chol.visible = true;
  if (r_y_caro) r_y_caro.visible = true;
  if (r_n) r_n.visible = true;
} if (filt_prov_y) {
  filt_y.booleans = arr;
  filt_y.change.emit();
}
""")
        sel_year.js_on_change('value', cb_year)
        controls.append(sel_year)

    if controls:
        show(column(*controls, p))
    else:
        show(p)

# --------------------------------------------------------------------------------------------------------------------------------------------------------

def plot_price_km_by_province_json(result, marca, modelo):
    """
    Grafica interactiva Precio vs Km coloreando por provincia (paleta pastel).
    - Colorea por provincia sÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³lo si hay 3+ anuncios por provincia; resto en gris.
    - Leyenda con provincias presentes (y categorÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â­as Chollo/Timo).
    - Mantiene selector por aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o y hover con Provincia.
    """
    import math
    try:
        from catalog.dict_prov import dict_prov
    except Exception:
        dict_prov = {}

    try:
        output_notebook(INLINE)
    except Exception:
        pass

    df = pd.DataFrame(result)
    registros = df.to_dict(orient='records')

    def _coerce_year(y):
        try:
            y = int(str(y).strip())
            if 1980 <= y <= 2035:
                return y
        except Exception:
            pass
        return None

    kms, precios, urls, years = [], [], [], []
    prov_names = []
    for it in registros:
        precio = it.get('price', None)
        km_val = it.get('km', None)
        url = it.get('url', None)
        year_val = _coerce_year(it.get('year', None))
        if precio is None or (isinstance(precio, float) and math.isnan(precio)):
            continue
        if km_val is None or (isinstance(km_val, float) and math.isnan(km_val)):
            continue
        kms.append(int(km_val))
        precios.append(float(precio))
        urls.append(url if isinstance(url, str) and url.strip() else 'javascript:void(0)')
        years.append(year_val)
        prov_raw = it.get('provinceId', None)
        try:
            prov_int = int(prov_raw) if prov_raw is not None and str(prov_raw).strip() != '' else None
        except Exception:
            prov_int = None
        prov_names.append(dict_prov.get(prov_int, (str(prov_raw) if prov_raw is not None else '-')))

    if len(kms) == 0:
        print('Sin datos vÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡lidos para graficar')
        return

    order = np.argsort(kms)
    kms_arr = np.array(kms)[order]
    precios_arr = np.array(precios)[order]
    urls_arr = np.array(urls, dtype=object)[order]
    years_arr = np.array(years, dtype=object)[order]
    prov_names_arr = np.array(prov_names, dtype=object)[order]

    def exp_func(x, a, b, c):
        return a * np.exp(b * x) + c

    fit_x, fit_y = None, None
    if len(kms_arr) >= 3:
        try:
            p0 = [max(precios_arr), -1e-4, min(precios_arr)]
            popt, _ = curve_fit(exp_func, kms_arr, precios_arr, p0=p0, maxfev=5000)
            x_line = np.linspace(float(kms_arr.min()) - 100, float(kms_arr.max()) + 100, 200)
            fit_x = x_line
            fit_y = exp_func(x_line, *popt)
            eq_text = f'y = {popt[0]:.0f}ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â·e^({popt[1]:.7f}ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â·x) + {popt[2]:.0f}'
        except Exception as e:
            eq_text = f'Ajuste no disponible: {e}'
    else:
        eq_text = 'Datos insuficientes para ajuste'

    x_log = np.log1p(kms_arr).astype(float)
    yrs_num = np.array([np.nan if y is None else float(y) for y in years_arr])
    mask_fit = ~np.isnan(yrs_num)
    if np.sum(mask_fit) >= 3:
        X = np.c_[np.ones(np.sum(mask_fit)), x_log[mask_fit], yrs_num[mask_fit]]
        yv = precios_arr[mask_fit]
        try:
            beta, *_ = np.linalg.lstsq(X, yv, rcond=None)
        except Exception:
            beta = np.array([np.median(yv), 0.0, 0.0])
        yr_fill = np.nanmedian(yrs_num[mask_fit])
        Xall = np.c_[np.ones(len(kms_arr)), x_log, np.where(np.isnan(yrs_num), yr_fill, yrs_num)]
        y_pred = Xall @ beta
        residuals = precios_arr - y_pred
        z_scores = np.zeros_like(residuals, dtype=float)
        unique_years = sorted(set(int(y) for y in yrs_num[mask_fit]))
        global_median = float(np.median(residuals[mask_fit])) if np.any(mask_fit) else 0.0
        global_mad = float(np.median(np.abs(residuals[mask_fit] - global_median))) or 1.0
        for yr in unique_years:
            idx = (yrs_num == yr)
            r = residuals[idx]
            if r.size >= 8:
                med = float(np.median(r))
                mad = float(np.median(np.abs(r - med))) or global_mad
                z_scores[idx] = (r - med) / (1.4826 * mad)
            else:
                z_scores[idx] = (r - global_median) / (1.4826 * global_mad)
        tags = []
        for rr, zz, yv in zip(residuals, z_scores, yrs_num):
            if np.isnan(yv):
                tags.append('sin_aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o')
            elif (zz <= -1.5) and (rr <= -1000):
                tags.append('chollo')
            elif (zz >= 1.5) and (rr >= 1000):
                tags.append('caro')
            else:
                tags.append('normal')
    else:
        residuals = np.zeros_like(precios_arr, dtype=float)
        z_scores = np.zeros_like(precios_arr, dtype=float)
        tags = ['sin_aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o' if (y is None) else 'normal' for y in years_arr]

    mask_year = np.array([y is not None for y in years_arr])

    def _fmt_eur(v): return f'{v:,.0f}ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬'.replace(',', '.')
    def _fmt_km(v): return f'{v:,.0f} km'.replace(',', '.')

    dark_blue = '#003366'
    green = '#2ECC71'
    red = '#E74C3C'

    # Coloreado por provincia (paleta pastel) para grupos con >=3 puntos
    unq, counts = np.unique(prov_names_arr, return_counts=True)
    count_map = {k: int(v) for k, v in zip(unq, counts)}
    prov_group_arr = np.array([p if (count_map.get(p, 0) >= 3 and str(p) != '0') else 'Otros' for p in prov_names_arr], dtype=object)
    factors = sorted([p for p in np.unique(prov_group_arr) if p != 'Otros'])
    pastel = list(Pastel1[9]) + list(Pastel2[8])
    if len(factors) > len(pastel):
        import math as _math
        rep = int(_math.ceil(len(factors) / len(pastel)))
        palette = (pastel * rep)[:len(factors)]
    else:
        palette = pastel[:len(factors)]
    color_map = {p: c for p, c in zip(factors, palette)}
    default_gray = '#BBBBBB'
    color_arr = np.array([color_map.get(g, default_gray) for g in prov_group_arr], dtype=object)

    border_colors = []
    for t in tags:
        if t == 'chollo': border_colors.append(green)
        elif t == 'caro': border_colors.append(red)
        else: border_colors.append(dark_blue)
    border_colors = np.array(border_colors, dtype=object)

    data_with_year = dict(
        km=kms_arr[mask_year],
        precio=precios_arr[mask_year],
        url=urls_arr[mask_year],
        year=[int(v) for v in years_arr[mask_year]],
        precio_fmt=[_fmt_eur(v) for v in precios_arr[mask_year]],
        km_fmt=[_fmt_km(v) for v in kms_arr[mask_year]],
        year_fmt=[str(int(v)) for v in years_arr[mask_year]],
        residual=residuals[mask_year],
        z=z_scores[mask_year],
        tag=[t for (t,m) in zip(tags, mask_year) if m],
        border_color=border_colors[mask_year],
        prov=[str(v) for v in prov_names_arr[mask_year]],
        prov_group=[str(v) for v in prov_group_arr[mask_year]],
        color=color_arr[mask_year],
    )
    data_no_year = dict(
        km=kms_arr[~mask_year],
        precio=precios_arr[~mask_year],
        url=urls_arr[~mask_year],
        year=[None]*int((~mask_year).sum()),
        precio_fmt=[_fmt_eur(v) for v in precios_arr[~mask_year]],
        km_fmt=[_fmt_km(v) for v in kms_arr[~mask_year]],
        year_fmt=['-']*int((~mask_year).sum()),
        prov=[str(v) for v in prov_names_arr[~mask_year]],
        prov_group=[str(v) for v in prov_group_arr[~mask_year]],
        color=color_arr[~mask_year],
    )

    source_y = ColumnDataSource(data_with_year)
    source_n = ColumnDataSource(data_no_year)

    black = '#000000'
    bright_blue = '#00BFFF'
    white = '#FFFFFF'
    gray = '#DDDDDD'

    p = figure(width=900, height=600, background_fill_color=white, toolbar_location='above')
    p.title = None
    p.add_layout(Title(text='RelaciÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³n Precio vs KilÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³metros', text_font_size='16pt', text_color=black), 'above')
    p.add_layout(Title(text=marca+":"+modelo, text_font_style='bold', text_font_size='20pt', text_color=bright_blue), 'above')

    p.min_border_top = 90
    p.axis.axis_line_color = black
    p.axis.major_tick_line_color = black
    p.axis.minor_tick_line_color = black
    p.axis.major_label_text_color = black
    p.xaxis.axis_label_text_color = black
    p.yaxis.axis_label_text_color = black
    p.grid.grid_line_color = gray
    p.grid.grid_line_alpha = 0.4
    p.xaxis.axis_label = 'KilÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³metros Recorridos (km)'
    p.yaxis.axis_label = 'Precio (ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬)'
    p.yaxis.formatter = NumeralTickFormatter(format='0,0')

    r_y_norm = r_y_chol = r_y_caro = None
    province_renderers = []
    if len(data_with_year['km']) > 0:
        filt_prov_y = BooleanFilter(booleans=[True]*len(data_with_year['km']))

        mask_tag_normal = [t == 'normal' for t in data_with_year['tag']]
        mask_tag_chollo = [t == 'chollo' for t in data_with_year['tag']]
        mask_tag_caro = [t == 'caro' for t in data_with_year['tag']]
        filt_tag_normal = BooleanFilter(booleans=mask_tag_normal)
        filt_tag_chollo = BooleanFilter(booleans=mask_tag_chollo)
        filt_tag_caro = BooleanFilter(booleans=mask_tag_caro)

        # Primero: Chollo y Timo en la leyenda (excluye 'Otros')
        prov_valid_with_year = [str(pg) != 'Otros' for pg in prov_group_arr[mask_year]]
        filt_prov_valid_y = BooleanFilter(booleans=prov_valid_with_year)
        view_chol = CDSView(source=source_y, filters=[filt_y, filt_tag_chollo, filt_prov_valid_y])
        view_caro = CDSView(source=source_y, filters=[filt_y, filt_tag_caro, filt_prov_valid_y])
        r_y_chol = p.circle('km', 'precio', size=10, fill_color=green, line_color='#006400', line_width=1.8,
                            alpha=0.95, source=source_y, view=view_chol, legend_label='Chollo')
        r_y_caro = p.circle('km', 'precio', size=10, fill_color=red, line_color='#800000', line_width=1.8,
                            alpha=0.95, source=source_y, view=view_caro, legend_label='Timo')

        # DespuÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â©s: provincias ordenadas por nÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Âºmero de registros (desc) y sin '0' ni 'Otros'
        # Construir mÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡scara de provincias por cada factor
        factors_sorted = sorted([p for p in factors if str(p) != '0'], key=lambda p: count_map.get(p, 0), reverse=True)
        for prov in factors_sorted:
            label = f"{prov} ({count_map.get(prov, 0)})"
            # mÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡scara por provincia restringida a entries con year (source_y)
            prov_mask_full = [str(pg) == str(prov) for pg in prov_group_arr]
            prov_mask_with_year = [v for (v, m) in zip(prov_mask_full, mask_year) if m]
            filt_prov = BooleanFilter(booleans=prov_mask_with_year)
            view_prov = CDSView(source=source_y, filters=[filt_y, filt_tag_normal, filt_prov])
            r = p.circle('km', 'precio', size=10, fill_color=color_map.get(prov, default_gray), line_color=dark_blue, line_width=1.5,
                         alpha=0.95, source=source_y, view=view_prov, legend_label=label)
            province_renderers.append(r)

        p.legend.location = 'top_right'
        p.legend.background_fill_color = 'white'
        p.legend.background_fill_alpha = 0.8
        p.legend.label_text_color = black
        p.legend.border_line_color = black
        p.legend.border_line_alpha = 0.3
        try:
            p.legend.title = 'Provincia (ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â°Ãƒâ€šÃ‚Â¥3)'
        except Exception:
            pass

    r_n = None
    if len(data_no_year['km']) > 0:
        # Excluir 'Otros' tambiÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â©n en puntos sin aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o
        prov_valid_no_year = [str(pg) != 'Otros' for pg in data_no_year.get('prov_group', [])]
        if prov_valid_no_year:
            view_n = CDSView(source=source_n, filters=[BooleanFilter(booleans=prov_valid_no_year)])
            r_n = p.circle('km', 'precio', size=9, color=black, line_color=dark_blue, line_width=1.5, alpha=0.8, source=source_n, view=view_n)

    if fit_x is not None and fit_y is not None:
        try:
            y_fit_pts = exp_func(kms_arr, *popt)
            resid = precios_arr - y_fit_pts
            mad = float(np.median(np.abs(resid - np.median(resid))))
            sigma = (1.4826*mad) if mad > 0 else float(np.std(resid))
            low = fit_y - 1.96*sigma
            high = fit_y + 1.96*sigma
            band_src = ColumnDataSource(dict(x=fit_x, low=low, high=high))
            band = Band(base='x', lower='low', upper='high', source=band_src, level='underlay', fill_color='#BBBBBB', fill_alpha=0.18, line_color=None)
            p.add_layout(band)
        except Exception:
            pass
        p.line(fit_x, fit_y, line_width=3, color=bright_blue)

    renderers_for_hover = [r for r in ([r_y_chol, r_y_caro] + province_renderers + ([r_n] if 'r_n' in locals() else [])) if r is not None]
    hover = HoverTool(renderers=renderers_for_hover, tooltips=[
        ('AÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o', '@year_fmt'),
        ('Km', '@km_fmt'),
        ('Precio', '@precio_fmt'),
        ('Provincia', '@prov'),
        ('URL', '@url')
    ])
    p.add_tools(hover)
    tap = TapTool()
    p.add_tools(tap)
    tap.callback = OpenURL(url='@url')

    if eq_text:
        label = Label(x=10, y=p.height - 10, x_units='screen', y_units='screen',
                      text=eq_text, text_color=black, text_font='courier', text_font_size='10pt',
                      background_fill_color=white, background_fill_alpha=0.8)
        p.add_layout(label)

    controls = []
    if 'year' in data_with_year and len(data_with_year['prov']) > 0:
        years_unique = sorted({int(y) for y in data_with_year['prov'] if y is not None})
        sel_year = Select(title='Filtrar por aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o', value='Todos',
                          options=(['Todos'] + [str(y) for y in years_unique] + (['Sin aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o'] if r_n is not None else [])))
        cb_year = CustomJS(args=dict(sel=sel_year, src_y=source_y, src_n=source_n, filt_prov_n=filt_prov_n,  src_n=source_n, filt_prov_n=filt_prov_n,  filt_prov_y=filt_prov_y if len(data_with_year['km'])>0 else None,
                                     r_n=r_n, r_y_norm=r_y_norm, r_y_chol=r_y_chol, r_y_caro=r_y_caro), code="""
const val = sel.value;
if (!src_y || !src_y.data || !src_y.data['prov']) return;
const years = src_y.data['prov'];
const n = provs.length;
let arr = new Array(n).fill(true);
if (val === 'Todos') {
  arr = Array(n).fill(true);
  if (r_y_norm) r_y_norm.visible = true;
  if (r_y_chol) r_y_chol.visible = true;
  if (r_y_caro) r_y_caro.visible = true;
  if (r_n) r_n.visible = true;
} if (filt_prov_y) {
  filt_y.booleans = arr;
  filt_y.change.emit();
}
""")
        sel_year.js_on_change('value', cb_year)
        controls.append(sel_year)

    if controls:
        show(column(*controls, p))
    else:
        show(p)



# --------------------------------------------------------------------------------------------------------------------------------------------------------

    





def plot_price_km_db(marca,modelo):
    """
    Grafica interactiva (Bokeh) Precio vs KilÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³metros para un modelo.
    Lee una lista de dicts con claves: id, url, title, km, price, year, imgUrl, provinceId.
    Mantiene el diseÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o/estilo original.
    """
    import math

    # Asegura que BokehJS se cargue en el contexto del notebook
    try:
        output_notebook(INLINE)
    except Exception:
        pass

    # === Preparar DataFrame y filtrar por modelo (ahora usamos 'title') ===
    registros = db_read_dict(tabla='data_moto', 
              campos=['title', 'km','price','year','url'], 
              condicion_sql = f"modelo = '{modelo}' and  marca = '{marca}' "
              )
    # === Lectura directa de campos: price, km, url, year ===
    def _coerce_year(y):
        try:
            y = int(str(y).strip())
            if 1980 <= y <= 2035:
                return y
        except Exception:
            pass
        return None

    kms, precios, urls, years = [], [], [], []
    for it in registros:
        precio = it.get('price', None)
        km_val = it.get('km', None)
        url = it.get('url', None)
        year_val = _coerce_year(it.get('year', None))

        if precio is None or (isinstance(precio, float) and math.isnan(precio)):
            continue
        if km_val is None or (isinstance(km_val, float) and math.isnan(km_val)):
            continue

        kms.append(int(km_val))
        precios.append(float(precio))
        urls.append(url if isinstance(url, str) and url.strip() else 'javascript:void(0)')
        years.append(year_val)

    if len(kms) == 0:
        print('Sin datos vÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡lidos para graficar')
        return

    # === Ordenar por km para lÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â­nea de ajuste (sin cambios) ===
    order = np.argsort(kms)
    kms_arr = np.array(kms)[order]
    precios_arr = np.array(precios)[order]
    urls_arr = np.array(urls, dtype=object)[order]
    years_arr = np.array(years, dtype=object)[order]

    # === Ajuste exponencial (igual) ===
    def exp_func(x, a, b, c):
        return a * np.exp(b * x) + c

    fit_x, fit_y = None, None
    if len(kms_arr) >= 3:
        try:
            p0 = [max(precios_arr), -1e-4, min(precios_arr)]
            popt, _ = curve_fit(exp_func, kms_arr, precios_arr, p0=p0, maxfev=5000)
            x_line = np.linspace(float(kms_arr.min()) - 100, float(kms_arr.max()) + 100, 200)
            fit_x = x_line
            fit_y = exp_func(x_line, *popt)
            eq_text = f'y = {popt[0]:.0f}ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â·e^({popt[1]:.7f}ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â·x) + {popt[2]:.0f}'
        except Exception as e:
            eq_text = f'Ajuste no disponible: {e}'
    else:
        eq_text = 'Datos insuficientes para ajuste'

    # === ClasificaciÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³n chollo/caro (igual) ===
    x_log = np.log1p(kms_arr).astype(float)
    yrs_num = np.array([np.nan if y is None else float(y) for y in years_arr])
    mask_fit = ~np.isnan(yrs_num)
    if np.sum(mask_fit) >= 3:
        X = np.c_[np.ones(np.sum(mask_fit)), x_log[mask_fit], yrs_num[mask_fit]]
        yv = precios_arr[mask_fit]
        try:
            beta, *_ = np.linalg.lstsq(X, yv, rcond=None)
        except Exception:
            beta = np.array([np.median(yv), 0.0, 0.0])
        yr_fill = np.nanmedian(yrs_num[mask_fit])
        Xall = np.c_[np.ones(len(kms_arr)), x_log, np.where(np.isnan(yrs_num), yr_fill, yrs_num)]
        y_pred = Xall @ beta
        residuals = precios_arr - y_pred
        z_scores = np.zeros_like(residuals, dtype=float)
        unique_years = sorted(set(int(y) for y in yrs_num[mask_fit]))
        global_median = float(np.median(residuals[mask_fit])) if np.any(mask_fit) else 0.0
        global_mad = float(np.median(np.abs(residuals[mask_fit] - global_median))) or 1.0
        for yr in unique_years:
            idx = (yrs_num == yr)
            r = residuals[idx]
            if r.size >= 8:
                med = float(np.median(r))
                mad = float(np.median(np.abs(r - med))) or global_mad
                z_scores[idx] = (r - med) / (1.4826 * mad)
            else:
                z_scores[idx] = (r - global_median) / (1.4826 * global_mad)
        tags = []
        for rr, zz, yv in zip(residuals, z_scores, yrs_num):
            if np.isnan(yv):
                tags.append('sin_aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o')
            elif (zz <= -1.5) and (rr <= -1000):
                tags.append('chollo')
            elif (zz >= 1.5) and (rr >= 1000):
                tags.append('caro')
            else:
                tags.append('normal')
    else:
        residuals = np.zeros_like(precios_arr, dtype=float)
        z_scores = np.zeros_like(precios_arr, dtype=float)
        tags = ['sin_aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o' if (y is None) else 'normal' for y in years_arr]

    # === ConstrucciÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³n de fuentes (igual) ===
    mask_year = np.array([y is not None for y in years_arr])

    def _fmt_eur(v): return f'{v:,.0f}ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬'.replace(',', '.')
    def _fmt_km(v): return f'{v:,.0f} km'.replace(',', '.')

    dark_blue = '#003366'
    green = '#2ECC71'
    red = '#E74C3C'
    border_colors = []
    for t in tags:
        if t == 'chollo': border_colors.append(green)
        elif t == 'caro': border_colors.append(red)
        else: border_colors.append(dark_blue)
    border_colors = np.array(border_colors, dtype=object)

    data_with_year = dict(
        km=kms_arr[mask_year],
        precio=precios_arr[mask_year],
        url=urls_arr[mask_year],
        year=[int(v) for v in years_arr[mask_year]],
        precio_fmt=[_fmt_eur(v) for v in precios_arr[mask_year]],
        km_fmt=[_fmt_km(v) for v in kms_arr[mask_year]],
        year_fmt=[str(int(v)) for v in years_arr[mask_year]],
        residual=residuals[mask_year],
        z=z_scores[mask_year],
        tag=[t for (t,m) in zip(tags, mask_year) if m],
        border_color=border_colors[mask_year],
    )
    data_no_year = dict(
        km=kms_arr[~mask_year],
        precio=precios_arr[~mask_year],
        url=urls_arr[~mask_year],
        year=[None]*int((~mask_year).sum()),
        precio_fmt=[_fmt_eur(v) for v in precios_arr[~mask_year]],
        km_fmt=[_fmt_km(v) for v in kms_arr[~mask_year]],
        year_fmt=['-']*int((~mask_year).sum()),
    )

    source_y = ColumnDataSource(data_with_year)
    source_n = ColumnDataSource(data_no_year)

    black = '#000000'
    blue = '#1E90FF'
    bright_blue = '#00BFFF'
    white = '#FFFFFF'
    gray = '#DDDDDD'

    # === TÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â­tulos (igual) ===
    p = figure(width=900, height=600, background_fill_color=white, toolbar_location='above')
    p.title = None
    p.add_layout(Title(text='RelaciÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³n Precio vs KilÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³metros', text_font_size='16pt', text_color=black), 'above')
    p.add_layout(Title(text=modelo, text_font_style='bold', text_font_size='20pt', text_color=bright_blue), 'above')

    # === EstÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â©tica (igual) ===
    p.min_border_top = 90
    p.axis.axis_line_color = black
    p.axis.major_tick_line_color = black
    p.axis.minor_tick_line_color = black
    p.axis.major_label_text_color = black
    p.xaxis.axis_label_text_color = black
    p.yaxis.axis_label_text_color = black
    p.grid.grid_line_color = gray
    p.grid.grid_line_alpha = 0.4
    p.xaxis.axis_label = 'KilÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³metros Recorridos (km)'
    p.yaxis.axis_label = 'Precio (ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬)'
    p.yaxis.formatter = NumeralTickFormatter(format='0,0')

    r_y_norm = r_y_chol = r_y_caro = None
    if len(data_with_year['km']) > 0:
        y_low = float(np.min(data_with_year['prov']))
        y_high = float(np.max(data_with_year['prov']))
        mapper = linear_cmap(field_name='year', palette=list(reversed(Blues256)), low=y_low, high=y_high)
        filt_y = BooleanFilter(booleans=[True]*len(data_with_year['km']))

        mask_tag_normal = [t == 'normal' for t in data_with_year['tag']]
        mask_tag_chollo = [t == 'chollo' for t in data_with_year['tag']]
        mask_tag_caro = [t == 'caro' for t in data_with_year['tag']]
        filt_tag_normal = BooleanFilter(booleans=mask_tag_normal)
        filt_tag_chollo = BooleanFilter(booleans=mask_tag_chollo)
        filt_tag_caro = BooleanFilter(booleans=mask_tag_caro)

        view_norm = CDSView(source=source_y, filters=[filt_prov_y, filt_tag_normal])
        view_chol = CDSView(source=source_y, filters=[filt_prov_y, filt_tag_chollo])
        view_caro = CDSView(source=source_y, filters=[filt_prov_y, filt_tag_caro])

        r_y_norm = p.circle('km', 'precio', size=10, color=mapper, line_color=dark_blue, line_width=1.5, alpha=0.95, source=source_y, view=view_norm)
        r_y_chol = p.circle('km', 'precio', size=10, fill_color=green, line_color='#006400', line_width=1.8, alpha=0.95, source=source_y, view=view_chol, legend_label='Chollo')
        r_y_caro = p.circle('km', 'precio', size=10, fill_color=red, line_color='#800000', line_width=1.8, alpha=0.95, source=source_y, view=view_caro, legend_label='Timo')

        p.legend.location = 'top_right'
        p.legend.background_fill_color = 'white'
        p.legend.background_fill_alpha = 0.8
        p.legend.label_text_color = black
        p.legend.border_line_color = black
        p.legend.border_line_alpha = 0.3

        cbar = ColorBar(color_mapper=mapper['transform'], title='AÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o', label_standoff=8)
        p.add_layout(cbar, 'right')

    r_n = None
    if len(data_no_year['km']) > 0:
        r_n = p.circle('km', 'precio', size=9, color=black, line_color=dark_blue, line_width=1.5, alpha=0.8, source=source_n)

    if fit_x is not None and fit_y is not None:
        try:
            y_fit_pts = exp_func(kms_arr, *popt)
            resid = precios_arr - y_fit_pts
            mad = float(np.median(np.abs(resid - np.median(resid))))
            sigma = (1.4826*mad) if mad > 0 else float(np.std(resid))
            low = fit_y - 1.96*sigma
            high = fit_y + 1.96*sigma
            band_src = ColumnDataSource(dict(x=fit_x, low=low, high=high))
            band = Band(base='x', lower='low', upper='high', source=band_src, level='underlay', fill_color='#BBBBBB', fill_alpha=0.18, line_color=None)
            p.add_layout(band)
        except Exception:
            pass
        p.line(fit_x, fit_y, line_width=3, color=blue)

    renderers_for_hover = [r for r in (r_y_norm, r_y_chol, r_y_caro, r_n) if r is not None]
    hover = HoverTool(renderers=renderers_for_hover, tooltips=[
        ('Precio', '@precio_fmt'),
        ('KilÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³metros', '@km_fmt'),
        ('AÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o', '@year_fmt'),
        ('URL', '@url')
    ])
    p.add_tools(hover)
    tap = TapTool()
    p.add_tools(tap)
    tap.callback = OpenURL(url='@url')

    if eq_text:
        label = Label(x=10, y=p.height - 10, x_units='screen', y_units='screen',
                      text=eq_text, text_color=black, text_font='courier', text_font_size='10pt',
                      background_fill_color=white, background_fill_alpha=0.8)
        p.add_layout(label)

    controls = []
    if 'year' in data_with_year and len(data_with_year['prov']) > 0:
        years_unique = sorted({int(y) for y in data_with_year['prov'] if y is not None})
        sel_year = Select(title='Filtrar por aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o', value='Todos',
                          options=(['Todos'] + [str(y) for y in years_unique] + (['Sin aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±o'] if r_n is not None else [])))
        cb_year = CustomJS(args=dict(sel=sel_year, src_y=source_y, src_n=source_n, filt_prov_n=filt_prov_n,  src_n=source_n, filt_prov_n=filt_prov_n,  filt_prov_y=filt_prov_y if len(data_with_year['km'])>0 else None,
                                     r_n=r_n, r_y_norm=r_y_norm, r_y_chol=r_y_chol, r_y_caro=r_y_caro), code="""
const val = sel.value;
if (!src_y || !src_y.data || !src_y.data['prov']) return;
const years = src_y.data['prov'];
const n = provs.length;
let arr = new Array(n).fill(true);
if (val === 'Todos') {
  arr = Array(n).fill(true);
  if (r_y_norm) r_y_norm.visible = true;
  if (r_y_chol) r_y_chol.visible = true;
  if (r_y_caro) r_y_caro.visible = true;
  if (r_n) r_n.visible = true;
} if (filt_prov_y) {
  filt_y.booleans = arr;
  filt_y.change.emit();
}
""")
        sel_year.js_on_change('value', cb_year)
        controls.append(sel_year)

    if controls:
        show(column(*controls, p))
    else:
        show(p)


