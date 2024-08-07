use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    compute::{cast, concat},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::{
    functions_aggregate::{count::count, sum::sum},
    prelude::{case, col, is_null, lit, DataFrame},
};

#[allow(dead_code)]
pub struct DescribeDataFrame {
    df: DataFrame,
    functions: &'static [&'static str],
    schema: SchemaRef,
}

#[allow(dead_code)]
impl DescribeDataFrame {
    pub fn new(df: DataFrame) -> Self {
        let functions = &["count", "null_count"];
        let describe_schemas = vec![Field::new("describe", DataType::Utf8, false)]
            .into_iter()
            .chain(df.schema().fields().iter().map(|field| {
                if field.data_type().is_numeric() {
                    Field::new(field.name(), DataType::Float64, true)
                } else {
                    Field::new(field.name(), DataType::Utf8, true)
                }
            }))
            .collect::<Vec<_>>();
        Self {
            df,
            functions,
            schema: Arc::new(Schema::new(describe_schemas)),
        }
    }

    fn count(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        Ok(self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .map(|f| count(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?)
    }

    fn null_count(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .map(|f| {
                    sum(case(is_null(col(f.name())))
                        .when(lit(true), lit(1))
                        .otherwise(lit(0))
                        .unwrap())
                    .alias(f.name())
                })
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }
    pub async fn to_record_batch(&self) -> anyhow::Result<RecordBatch> {
        // 原始 DataFrame所有的列
        let original_schema_fields = self.df.schema().fields().iter();

        // 使用所有列分别计算出来的指标，这里是两行数据
        let batches = vec![self.count(), self.null_count()];

        // 指标名这一列
        let mut describe_col_vec: Vec<ArrayRef> = vec![Arc::new(StringArray::from(
            self.functions
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>(),
        ))];
        // 遍历每一个列字段
        for field in original_schema_fields {
            let mut array_data = vec![];
            // 遍历每一个指标
            for result in batches.iter() {
                let array_ref = match result {
                    Ok(df) => {
                        let batchs = df.clone().collect().await;
                        match batchs {
                            // 什么情况下len() 不等于 1
                            // batchs[0]包含所有列的数据
                            Ok(batchs)
                                if batchs.len() == 1
                                    && batchs[0].column_by_name(field.name()).is_some() =>
                            {
                                let column = batchs[0].column_by_name(field.name()).unwrap();
                                if field.data_type().is_numeric() {
                                    cast(column, &DataType::Float64)?
                                } else {
                                    cast(column, &DataType::Utf8)?
                                }
                            }
                            _ => Arc::new(StringArray::from(vec!["null"])),
                        }
                    }
                    // 如果指标计算出错，则返回 null
                    Err(err)
                        if err.to_string().contains(
                            "Error during planning: \
                    Aggregate requires at least one grouping \
                    or aggregate expression",
                        ) =>
                    {
                        Arc::new(StringArray::from(vec!["null"]))
                    }
                    Err(other_err) => {
                        panic!("{other_err}")
                    }
                };
                array_data.push(array_ref);
            }
            describe_col_vec.push(concat(
                array_data
                    .iter()
                    .map(|af| af.as_ref())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )?);
        }
        let batch = RecordBatch::try_new(self.schema.clone(), describe_col_vec)?;

        Ok(batch)
    }
}
