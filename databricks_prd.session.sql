Select *
From mention_dw.etl_fact_pipeline_config;
Delete From mention_dw.etl_fact_pipeline_config
Where table_name = 'fact_inventory_snapshot';
-- Update mention_dw.etl_fact_pipeline_config
-- Set date_control = '2000-01-01';