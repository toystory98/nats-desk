// Apply the saved preference before CSS paints to avoid a flash of the wrong theme.
(function () {
  const prefs = window.natsDeskPrefs = { themeKey: 'nats-desk-theme', scaleKey: 'nats-desk-ui-scale', scales: [0.9, 1, 1.1, 1.25, 1.5] };
  let theme, scale;
  try { theme = localStorage.getItem(prefs.themeKey); scale = Number(localStorage.getItem(prefs.scaleKey)); } catch (_) {}
  if (theme !== 'light' && theme !== 'dark') {
    theme = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }
  document.documentElement.dataset.theme = theme;
  if (scale !== 1 && prefs.scales.includes(scale)) document.documentElement.style.setProperty('--ui-scale', String(scale));
})();
