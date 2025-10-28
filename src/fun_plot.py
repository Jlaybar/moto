import numpy as np
import pandas as pd
from scipy.optimize import curve_fit

from bokeh.io import output_notebook, show
from bokeh.plotting import figure
from bokeh.models import (
    ColumnDataSource, HoverTool, TapTool, OpenURL, NumeralTickFormatter, Label, ColorBar,
    Select, CustomJS, CDSView, BooleanFilter, Band, Title
)
from bokeh.transform import linear_cmap
from bokeh.palettes import Blues256
from bokeh.layouts import column

output_notebook()

def plot_price_km_v1(result, MODELO):
    """
    Grafica interactiva (Bokeh) Precio vs Kilómetros para un MODELO.
    - Fondo blanco, textos/etiquetas en negro; grid gris suave.
    - Puntos rellenos en degradado azul por año (más oscuro = más antiguo).
    - Borde azul oscuro; verde/rojo si es chollo/caro.
    - Barra de color Año.
    - Selector: Año (filtra).
    - Hover: Precio, Km, Año, URL; Tap: abre URL.
    """
    import math, re

    # Preparar DataFrame y filtrar por modelo
    df = pd.DataFrame(result)
    if 'name' in df.columns:
        df = df[df['name'].astype(str).str.contains(MODELO, case=False, na=False)]

    registros = df.to_dict(orient='records')

    def _parse_year(item):
        for k in ('year','anio','año','model_year','year_model','registration_year'):
            if k in item and item[k] is not None:
                try:
                    y = int(str(item[k]).strip())
                    if 1980 <= y <= 2035:
                        return y
                except Exception:
                    pass
        attrs = item.get('attributes')
        if isinstance(attrs, (list, tuple)):
            for s in attrs:
                if not isinstance(s, str):
                    continue
                m = re.search(r'(19|20)\d{2}', s)
                if m:
                    y = int(m.group(0))
                    if 1980 <= y <= 2035:
                        return y
        name = item.get('name')
        if isinstance(name, str):
            m = re.search(r'(19|20)\d{2}', name)
            if m:
                y = int(m.group(0))
                if 1980 <= y <= 2035:
                    return y
        return None

    kms, precios, urls, years = [], [], [], []
    for it in registros:
        precio = it.get('price')
        if precio is None or (isinstance(precio, float) and math.isnan(precio)):
            continue

        km_val = None
        attrs = it.get('attributes')
        if isinstance(attrs, (list, tuple)):
            for attr in attrs:
                if not isinstance(attr, str):
                    continue
                if 'km' in attr.lower():
                    try:
                        s = attr.split(' km')[0].replace('.', '').replace(',', '').strip()
                        km_val = int(s)
                        break
                    except Exception:
                        continue

        url = None
        for k in ('url','permalink','perma_link','link','href','permalink_url'):
            v = it.get(k)
            if isinstance(v, str) and v.strip():
                url = v.strip()
                break

        year_val = _parse_year(it)

        if km_val is not None and precio is not None:
            kms.append(int(km_val))
            precios.append(float(precio))
            urls.append(url or 'javascript:void(0)')
            years.append(year_val)

    if len(kms) == 0:
        print('Sin datos válidos para graficar')
        return

    # Ordenar por km para línea de ajuste
    order = np.argsort(kms)
    kms_arr = np.array(kms)[order]
    precios_arr = np.array(precios)[order]
    urls_arr = np.array(urls, dtype=object)[order]
    years_arr = np.array(years, dtype=object)[order]

    # Ajuste exponencial (visual)
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
            eq_text = f'y = {popt[0]:.0f}·e^({popt[1]:.7f}·x) + {popt[2]:.0f}'
        except Exception as e:
            eq_text = f'Ajuste no disponible: {e}'
    else:
        eq_text = 'Datos insuficientes para ajuste'

    # Clasificación chollo/caro (modelo simple OLS robustecida con MAD)
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
                tags.append('sin_año')
            elif (zz <= -1.5) and (rr <= -1000):
                tags.append('chollo')
            elif (zz >= 1.5) and (rr >= 1000):
                tags.append('caro')
            else:
                tags.append('normal')
    else:
        residuals = np.zeros_like(precios_arr, dtype=float)
        z_scores = np.zeros_like(precios_arr, dtype=float)
        tags = ['sin_año' if (y is None) else 'normal' for y in years_arr]

    # Construcción de fuentes: con año y sin año
    mask_year = np.array([y is not None for y in years_arr])

    def _fmt_eur(v): return f'{v:,.0f}€'.replace(',', '.')
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

    # MODIFICACIÓN PRINCIPAL: Configuración del título con subtítulo
    p = figure(width=900, height=600, background_fill_color=white, toolbar_location='above')
    
    # Eliminar título por defecto
    p.title = None
    
    # Añadir título principal en azul
    p.add_layout(Title(
        text='Relación Precio vs Kilómetros', 
        text_font_size='16pt',  # Mismo tamaño que el título
        text_color=black
    ), 'above')

    p.add_layout(Title(
        text=MODELO, 
        text_font_style='bold', 
        text_font_size='20pt',
        text_color=bright_blue
    ), 'above')
    

    # Configuración del resto de propiedades (manteniendo tu código original)
    p.min_border_top = 90  # Aumentado para acomodar ambos títulos
    p.axis.axis_line_color = black
    p.axis.major_tick_line_color = black
    p.axis.minor_tick_line_color = black
    p.axis.major_label_text_color = black
    p.xaxis.axis_label_text_color = black
    p.yaxis.axis_label_text_color = black
    p.grid.grid_line_color = gray
    p.grid.grid_line_alpha = 0.4

    p.xaxis.axis_label = 'Kilómetros Recorridos (km)'
    p.yaxis.axis_label = 'Precio (€)'
    p.yaxis.formatter = NumeralTickFormatter(format='0,0')

    r_y = None
    if len(data_with_year['km']) > 0:
        y_low = float(np.min(data_with_year['year']))
        y_high = float(np.max(data_with_year['year']))
        mapper = linear_cmap(field_name='year', palette=list(reversed(Blues256)), low=y_low, high=y_high)
        filt_y = BooleanFilter(booleans=[True]*len(data_with_year['km']))
        # Vistas por etiqueta para colorear relleno/borde según chollo/caro/normal
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
        cbar = ColorBar(color_mapper=mapper['transform'], title='Año', label_standoff=8)
        p.add_layout(cbar, 'right')

    r_n = None
    if len(data_no_year['km']) > 0:
        r_n = p.circle('km', 'precio', size=9, color=black, line_color=dark_blue, line_width=1.5, alpha=0.8, source=source_n)

    if fit_x is not None and fit_y is not None:
        # Banda de dispersión (gris tenue) alrededor de la curva de ajuste
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
        ('Kilómetros', '@km_fmt'),
        ('Año', '@year_fmt'),
        ('URL', '@url')
    ])
    p.add_tools(hover)
    tap = TapTool()
    p.add_tools(tap)
    tap.callback = OpenURL(url='@url')

    if eq_text:
        label = Label(x=10, y=p.height - 10, x_units='screen', y_units='screen', text=eq_text,
                      text_color=black, text_font='courier', text_font_size='10pt',
                      background_fill_color=white, background_fill_alpha=0.8)
        p.add_layout(label)

    controls = []
    if r_y_norm is not None or r_y_chol is not None or r_y_caro is not None:
        years_unique = sorted({int(y) for y in data_with_year['year'] if y is not None})
        sel_year = Select(title='Filtrar por año', value='Todos', options=(['Todos'] + [str(y) for y in years_unique] + (['Sin año'] if r_n is not None else [])))
        cb_year = CustomJS(args=dict(sel=sel_year, src_y=source_y, filt_y=filt_y, r_n=r_n, r_y_norm=r_y_norm, r_y_chol=r_y_chol, r_y_caro=r_y_caro), code="""
const val = sel.value;
const years = src_y.data['year'];
const n = years.length;
let arr = new Array(n).fill(true);
if (val === 'Todos') {
  arr = Array(n).fill(true);
  if (r_y_norm) r_y_norm.visible = true;
  if (r_y_chol) r_y_chol.visible = true;
  if (r_y_caro) r_y_caro.visible = true;
  if (r_n) r_n.visible = true;
} else if (val === 'Sin año' || val === 'Sin a\u00f1o') {
  arr = Array(n).fill(false);
  if (r_y_norm) r_y_norm.visible = false;
  if (r_y_chol) r_y_chol.visible = false;
  if (r_y_caro) r_y_caro.visible = false;
  if (r_n) r_n.visible = true;
} else {
  const y = parseInt(val);
  for (let i=0;i<n;i++) arr[i] = (parseInt(years[i]) === y);
  if (r_y_norm) r_y_norm.visible = true;
  if (r_y_chol) r_y_chol.visible = true;
  if (r_y_caro) r_y_caro.visible = true;
  if (r_n) r_n.visible = false;
}
filt_y.booleans = arr;
filt_y.change.emit();
""")
        sel_year.js_on_change('value', cb_year)
        controls.append(sel_year)

    if controls:
        show(column(*controls, p))
    else:
        show(p)