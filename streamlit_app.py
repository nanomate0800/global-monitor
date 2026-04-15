"""Streamlit entry point for Global Monitor.

The app itself is a static HTML/JS bundle under ./static/. This entry point
simply embeds it in an iframe. Streamlit Cloud serves ./static/ at
`/app/static/` when `enableStaticServing` is true in .streamlit/config.toml,
so relative fetches inside the iframe (e.g. `data/meta.json`) resolve
correctly against the iframe's own URL.
"""
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="Global Monitor",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Strip Streamlit's chrome so the embedded app uses the full viewport.
st.markdown(
    """
    <style>
      [data-testid="stHeader"] { display: none; }
      [data-testid="stSidebarNav"] { display: none; }
      #MainMenu, footer { visibility: hidden; }
      .block-container {
        padding: 0 !important;
        margin: 0 !important;
        max-width: 100% !important;
      }
      iframe { height: 100vh !important; min-height: 100vh !important; border: 0 !important; }
      html, body { overflow: hidden !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

# The iframe src resolves to <app-domain>/app/static/index.html — a real URL,
# which lets the app's relative fetches work as expected.
components.iframe(
    src="app/static/index.html",
    height=1000,
    scrolling=True,
)
