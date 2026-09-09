// Apply the saved preference before CSS paints to avoid a flash of the wrong theme.
(function () {
  let theme;
  try { theme = localStorage.getItem('nats-desk-theme'); } catch (_) {}
  if (theme !== 'light' && theme !== 'dark') {
    theme = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }
  document.documentElement.dataset.theme = theme;
})();
