module.exports = {
  content: [
    "./templates/**/*.html",
    "./static/**/*.js",
  ],
  safelist: [
    // Fractional spacing values the scanner misses due to the decimal point
    "py-1.5", "px-1.5", "gap-1.5", "space-y-1.5",
    "mt-1.5", "mb-1.5", "ml-1.5", "mr-1.5",
    "pt-1.5", "pb-1.5", "pl-1.5", "pr-1.5",
    // h-14 (nav height) — used as inline style now but kept for reference
    "h-14",
  ],
  theme: { extend: {} },
  plugins: [],
}
