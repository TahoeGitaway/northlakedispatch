module.exports = {
  content: [
    "./templates/**/*.html",
    "./static/**/*.js",
  ],
  safelist: [
    // All .5-step fractional values the scanner misses due to the decimal point
    // 0.5
    "p-0.5", "px-0.5", "py-0.5", "pt-0.5", "pb-0.5", "pl-0.5", "pr-0.5",
    "mt-0.5", "mb-0.5", "ml-0.5", "mr-0.5",
    "gap-0.5", "space-y-0.5", "space-x-0.5",
    "w-0.5", "h-0.5",
    // 1.5
    "p-1.5", "px-1.5", "py-1.5", "pt-1.5", "pb-1.5", "pl-1.5", "pr-1.5",
    "mt-1.5", "mb-1.5", "ml-1.5", "mr-1.5",
    "gap-1.5", "space-y-1.5", "space-x-1.5",
    "w-1.5", "h-1.5",
    // 2.5
    "p-2.5", "px-2.5", "py-2.5", "pt-2.5", "pb-2.5", "pl-2.5", "pr-2.5",
    "mt-2.5", "mb-2.5", "ml-2.5", "mr-2.5",
    "gap-2.5", "space-y-2.5", "space-x-2.5",
    "w-2.5", "h-2.5",
    // 3.5
    "p-3.5", "px-3.5", "py-3.5", "pt-3.5", "pb-3.5", "pl-3.5", "pr-3.5",
    "mt-3.5", "mb-3.5", "ml-3.5", "mr-3.5",
    "gap-3.5", "space-y-3.5", "space-x-3.5",
    "w-3.5", "h-3.5",
    // Fixed heights used by nav/layout
    "h-14",
  ],
  theme: { extend: {} },
  plugins: [],
}
