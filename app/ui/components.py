"""
Reusable, theme-aware UI building blocks for the IaC Orchestrator dashboard.

These intentionally build on ``ui.theme.UIStyles`` (the Lyndrix design system)
instead of hardcoding backgrounds, so light/dark mode is handled by the central
``lyndrix-card`` theming rather than ad-hoc Tailwind ``bg-white`` classes.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Optional

from nicegui import ui
from ui.theme import UIStyles


# Themed card surface (respects light/dark via the `lyndrix-card` rule).
CARD = UIStyles.CARD_BASE + " !p-0"


@contextmanager
def tile(color: str = "indigo", *, inner: str = "w-full p-4 gap-2",
         card_extra: str = "", hover: bool = True, glass: bool = False,
         stripe_color: Optional[str] = None):
    """A themed tile matching the app design language (core dashboard / Assignments).

    Sharp ``lyndrix-card`` surface with zeroed padding, a top accent stripe,
    and an inner content column — the same chrome every other tile in the app
    uses. Yields inside the inner column so callers just add content.

    ``stripe_color``, when given, is a raw CSS colour (e.g. from
    :func:`status_var`) that overrides the top stripe instead of the
    categorical ``color`` stem — used for status-driven tiles (job cards) so
    up/down/accent/muted job states don't have to fake a stem colour.
    """
    base = UIStyles.CARD_GLASS if glass else UIStyles.CARD_BASE
    hover_cls = " hover:border-[color-mix(in_srgb,var(--lx-accent)_50%,transparent)] transition-all" if hover else ""
    with ui.card().classes(f"{base}{hover_cls} {card_extra}".strip()).style(
        "padding: 0; flex-wrap: nowrap; min-width: 0"
    ):
        stripe = ui.element("div").classes("h-1 w-full")
        if stripe_color:
            stripe.style(f"background: {stripe_color}")
        else:
            stripe.classes(accent_grad(color))
        with ui.column().classes(inner):
            yield


# ----------------------------------------------------------------------------
# Categorical stem palette → --lx-chart-1..8 (Theming v2 T1).
#
# Single source of truth for the "stem" colour name used across this plugin's
# categorical data (pipeline phases, job types): ``pipeline_meta.PhaseDef``/
# ``PipelineTypeDef.color`` fields, and the React ``STEM_COLORS`` map in
# ``src/ui/PluginApp.tsx`` MUST use the exact same 8 keys mapped to the exact
# same ``--lx-chart-N`` token, in the exact same order, so the two GUIs (and
# any future one) never drift apart again. "zinc" is not a chart hue — it is
# the neutral fallback for uncategorised/"other" stems.
# ----------------------------------------------------------------------------
_STEM_CHART_VAR = {
    "violet":  "--lx-chart-1",
    "sky":     "--lx-chart-2",
    "emerald": "--lx-chart-3",
    "amber":   "--lx-chart-4",
    "rose":    "--lx-chart-5",
    "indigo":  "--lx-chart-6",
    "cyan":    "--lx-chart-7",
    "teal":    "--lx-chart-8",
}
_STEM_FALLBACK_VAR = "--lx-text-muted"  # zinc / unknown stems


def _stem_var(color: str) -> str:
    return _STEM_CHART_VAR.get(color, _STEM_FALLBACK_VAR)


def accent_text(color: str) -> str:
    """Tailwind arbitrary-value text-colour class for a stem, token-driven."""
    return f"text-[var({_stem_var(color)})]"


def accent_grad(color: str) -> str:
    """Tailwind arbitrary-value background class for a stem's accent stripe.

    Token-driven, single-hue (replaces the old hand-rolled 3-stop Tailwind
    gradients, which had drifted out of sync with the React ``STEM_COLORS``
    map and didn't respond to theme changes).
    """
    return f"bg-[var({_stem_var(color)})]"


# ----------------------------------------------------------------------------
# Job/pipeline status → theme state (Theming v2 T1).
#
# Single source of truth for "what colour is this job status" — replaces the
# THREE maps that used to drift independently (this file's old
# ``status_badge()`` colour/icon table, ``dashboard.py``'s local
# ``strip_color``, and ``overview_dashboard.py``'s local ``palette``). Mirrors
# the React ``statusColor()`` / ``badgeVariant()`` in ``PluginApp.tsx`` exactly
# so both stacks render job status identically.
# ----------------------------------------------------------------------------
_RUNNING_STATUSES = {"RUNNING", "PENDING"}
_FAIL_STATUSES = {"FAILED", "ERROR", "ABORTED"}

_STATUS_STATE_VAR = {
    "up": "--lx-state-up",
    "down": "--lx-state-down",
    "accent": "--lx-accent",
    "muted": "--lx-text-muted",
}


def status_state(status: str) -> str:
    """Bucket a raw job status into up/down/accent/muted."""
    s = (status or "").upper()
    if s == "SUCCESS":
        return "up"
    if s in _FAIL_STATUSES:
        return "down"
    if s in _RUNNING_STATUSES:
        return "accent"
    return "muted"


def status_var(status: str) -> str:
    """CSS var() reference for a raw job status (tile stripes, progress bars)."""
    return f"var({_STATUS_STATE_VAR[status_state(status)]})"


def kpi_card(label: str, value: str, *, icon: str, color: str = "indigo",
             sub: Optional[str] = None):
    """A compact KPI tile: app card chrome, accent icon, big mono value, optional sub."""
    text_c = accent_text(color)
    with tile(color, inner="w-full p-4 gap-1", hover=False, card_extra="min-w-0"):
        with ui.row().classes("w-full items-center justify-between no-wrap"):
            ui.label(label).classes(UIStyles.LABEL_MINI + " truncate")
            ui.icon(icon, size="18px").classes(text_c)
        ui.label(value).classes(
            f"text-3xl font-black font-mono leading-none {text_c}"
        )
        if sub:
            ui.label(sub).classes("text-xs text-[var(--lx-text-muted)] truncate")


def status_badge(status: str):
    """Coloured status pill — token-driven via the shared ``.lx-badge`` family,
    mirroring the React ``StatusBadge``/``badgeVariant()`` so both stacks stay
    in sync (see :func:`status_state`)."""
    s = (status or "").upper()
    variant = status_state(s)
    with ui.row().classes(
        f"{UIStyles.BADGE_STATE} lx-badge--{variant} no-wrap"
    ):
        ui.element("span").classes("lx-dot")
        ui.label(s or "—")


def progress_bar(percent: float, color: str = "indigo", *, var_override: Optional[str] = None):
    """A thin progress bar (0..100) on a themed track.

    ``var_override`` (e.g. from :func:`status_var`) draws a flat fill in that
    raw CSS colour instead of the categorical ``color`` stem.
    """
    pct = max(0.0, min(100.0, float(percent or 0)))
    with ui.element("div").classes(
        "w-full h-1.5 rounded-[var(--lx-radius-full)] bg-[var(--lx-elevated)] overflow-hidden"
    ):
        bar = ui.element("div").classes("h-full rounded-[var(--lx-radius-full)] transition-all")
        if var_override:
            bar.style(f"width: {pct}%; background: {var_override}")
        else:
            bar.classes(accent_grad(color)).style(f"width: {pct}%")


def section_header(title: str, subtitle: str = "", icon: Optional[str] = None,
                   color: str = "indigo"):
    """Section header in the app style: a vertical accent bar + title.

    Mirrors the core dashboard stack headers (``h-* w-1``), keeping an
    optional accent icon for context. The bar colour is the stem's chart
    token (see :func:`accent_grad`).
    """
    grad = accent_grad(color)
    with ui.row().classes("w-full items-center gap-3"):
        ui.element("div").classes(f"h-9 w-1 {grad} shrink-0")
        if icon:
            ui.icon(icon, size="20px").classes(accent_text(color))
        with ui.column().classes("gap-0"):
            ui.label(title).classes(UIStyles.TITLE_H3)
            if subtitle:
                ui.label(subtitle).classes(UIStyles.TEXT_MUTED + " !text-xs")
