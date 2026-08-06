cloudbio_base_theme <- function() {
  ggplot2::theme_minimal(base_family = "sans", base_size = 10.2) +
    ggplot2::theme(
      plot.title = ggplot2::element_text(size = 12, face = "bold", margin = ggplot2::margin(b = 4)),
      plot.subtitle = ggplot2::element_text(size = 9.5, lineheight = 1.05, margin = ggplot2::margin(b = 6)),
      axis.title = ggplot2::element_text(size = 10),
      axis.text = ggplot2::element_text(size = 9),
      axis.ticks = ggplot2::element_line(color = "#4D4D4D", linewidth = 0.25),
      axis.ticks.length = grid::unit(2, "pt"),
      axis.line = ggplot2::element_line(color = "#4D4D4D", linewidth = 0.25),
      panel.grid = ggplot2::element_blank(),
      legend.title = ggplot2::element_text(size = 9),
      legend.text = ggplot2::element_text(size = 9),
      legend.key.height = grid::unit(11, "pt"),
      legend.spacing.y = grid::unit(3, "pt"),
      plot.title.position = "plot",
      plot.caption.position = "plot",
      plot.margin = ggplot2::margin(10, 30, 12, 18, "pt")
    )
}

cloudbio_tag_theme <- function() {
  ggplot2::theme(plot.tag = ggplot2::element_text(face = "bold", size = 14))
}

cloudbio_palette <- list(
  ok = "#1B9E77",
  fail = "#D95F02",
  accent = "#4C78A8",
  blue_dark = "#08306b",
  blue_light = "#4292c6",
  gray_na = "#BDBDBD"
)
