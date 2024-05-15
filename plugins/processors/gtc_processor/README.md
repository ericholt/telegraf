# Join Processor Plugin

The join processor plugin creates a persistent copy of metric tags,
preserving untouched the original metric and allowing joining of tags
in other metrics on specific tag matches.

Select the metrics to modify using the standard [metric
filtering](../../../docs/CONFIGURATION.md#metric-filtering) options.

A typical use-case is extracting metric tags from certain metrics and storing
them in memory to then apply to other metrics from any incoming batch.

## Global configuration options `<!-- @/docs/includes/plugin_config.md -->`

In addition to the plugin-specific configuration settings, plugins support
additional global and plugin configuration settings. These settings are used to
modify metrics, tags, and field or create aliases and configure ordering, etc.
See the [CONFIGURATION.md][CONFIGURATION.md] for more details.

## Configuration

```toml
# 
[[processors.join]]
  namepass = ["kube_pod_.*"]
  ## 
  [processors.join.store]
  measurement = "kube_pod_annotations"
  # key = value of 'pod' tag
  store_match_tag = "pod"
  # value = value of 'annotation_prometheus_io_label_service' tag
  store_copy_tag = "annotation_prometheus_io_label_service"

  ## 
  [processors.join.copy]
  # matches 'pod' tag value to stored key
  match_tag = "pod"
  # copies value to tag 'service'
  copy_tag = "service"
```

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins
