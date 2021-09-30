module.exports = {
  docsSidebar: [
    'intro',
    'roadmap',
    {
      type: "category",
      label: "Guides",
      items: [
        "guides/overview",
        "guides/create_dagger",
        "guides/query_examples",
        "guides/use_transformer",
        "guides/use_udf",
        "guides/deployment",
        "guides/monitoring",
        "guides/troubleshooting"
      ],
    },
    {
      type: "category",
      label: "Concepts",
      items: [
        "concepts/overview",
        "concepts/basics",
        "concepts/lifecycle",
        "concepts/architecture"
      ],
    },
    {
      type: "category",
      label: "Advance",
      items: [
        "advance/overview",
        "advance/pre_processor",
        "advance/post_processor",
        "advance/longbow",
        "advance/longbow_plus",
        "advance/DARTS",
      ],
    },
    {
      type: "category",
      label: "Usecases",
      items: [
        "usecase/overview",
        "usecase/api_monitoring",
        "usecase/feature_ingestion",
        "usecase/stream_enrichment",
      ],
    },
    {
      type: "category",
      label: "Reference",
      items: [
        "reference/overview",
        "reference/configuration",
        "reference/metrics",
        "reference/transformers",
        "reference/udfs"
      ],
    },
    {
      type: "category",
      label: "Contribute",
      items: [
        "contribute/contribution",
        "contribute/development",
        "contribute/add_transformer",
        "contribute/add_udf",
      ],
    },

  ],
};