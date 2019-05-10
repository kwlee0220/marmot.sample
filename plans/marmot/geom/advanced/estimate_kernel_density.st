{
  "name": "EstimateKernelDensity 테스트",
  "operators": [
    <load_grid("주민/인구밀도_2000", "1000", "1000")>,
    <estimate_kernel_density("주민/인구밀도_2000", "value", "1000", "value")>
  ]
}