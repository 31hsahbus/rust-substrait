use arrow_schema::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_expr::WindowUDF;
use datafusion_expr::{
    logical_plan::builder::LogicalTableSource, AggregateUDF, ScalarUDF, TableSource,
};
use datafusion_sql::{
    planner::{ContextProvider, SqlToRel},
    sqlparser::{dialect::GenericDialect, parser::Parser},
    TableReference,
};
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_optimizer::{OptimizerContext, OptimizerRule};
use datafusion_expr::LogicalPlan;
use datafusion::prelude::{SessionContext};
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use std::{collections::HashMap, sync::Arc};
use warp::{Filter, Rejection, Reply};
use hyper::body::Bytes;
use warp::http::StatusCode;
use datafusion_proto::bytes::{logical_plan_from_bytes, logical_plan_to_bytes};
use warp::reject::Reject;

#[derive(Debug)]
struct DataFusionReject(DataFusionError);

impl Reject for DataFusionReject {}

struct MyContextProvider {
    options: ConfigOptions,
    tables: HashMap<String, Arc<dyn TableSource>>,
}

impl MyContextProvider {
    fn new() -> Self {
        let mut tables = HashMap::new();
        tables.insert(
            "customer".to_string(),
            create_table_source(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("first_name", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
                Field::new("state", DataType::Utf8, false),
            ]),
        );
        tables.insert(
            "state".to_string(),
            create_table_source(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("sales_tax", DataType::Decimal128(10, 2), false),
            ]),
        );
        tables.insert(
            "orders".to_string(),
            create_table_source(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("customer_id", DataType::Int32, false),
                Field::new("item_id", DataType::Int32, false),
                Field::new("quantity", DataType::Int32, false),
                Field::new("price", DataType::Decimal128(10, 2), false),
            ]),
        );
        Self {
            tables,
            options: Default::default(),
        }
    }
}

fn create_table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
    Arc::new(LogicalTableSource::new(Arc::new(
        Schema::new_with_metadata(fields, HashMap::new()),
    )))
}

impl ContextProvider for MyContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        match self.tables.get(name.table()) {
            Some(table) => Ok(table.clone()),
            _ => plan_err!("Table not found: {}", name.table()),
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }
}

fn observe(plan: &LogicalPlan, rule: &dyn OptimizerRule) {
    println!(
        "Applied rule: '{}'", rule.name()
    );
}

async fn get_substrait_ir(body: Bytes) -> Result<impl Reply, Rejection> {
    let body_str = String::from_utf8_lossy(&body).to_string();
    let sql = url::form_urlencoded::parse(body_str.as_bytes())
        .find(|(key, _)| key == "sql")
        .map(|(_, value)| value.to_string())
        .unwrap_or_else(|| String::new());
    //let sql = "select first_name, last_name, sales_tax, item_id from customer, state, orders where customer.id=state.id and orders.id=state.id and sales_tax = 100";
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, &sql).unwrap();
    let statement = &ast[0];

    let context_provider = MyContextProvider::new();
    let sql_to_rel = SqlToRel::new(&context_provider);

    let logical_plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();

    let mut config = OptimizerContext::default();
    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(&logical_plan, &config, &observe).unwrap();

    let ctx = SessionContext::new();
    //let substrait_ir = to_substrait_plan(&optimized_plan, &ctx).unwrap();
    let substrait_rel = logical_plan_to_bytes(&logical_plan);
    //let substrait_rel = logical_plan_to_bytes(&logical_plan).map_err(|err| DataFusionReject(err))?;
    println!("{optimized_plan:?}");
    println!("{substrait_rel:?}");
    // Convert your response to bytes
    let response_bytes = format!("{:?}", optimized_plan);
    //let response_bytes1: &[u8] = response_bytes.as_bytes();
    // Use warp::reply::with_status to return bytes with a specific HTTP status code
    //let response_bytes: Vec<u8> = format!("{:?}", substrait_ir).into_bytes();
    //println!("{response_bytes:?}");
    Ok(warp::reply::with_status(response_bytes, StatusCode::OK))
}

#[tokio::main]
async fn main() {
    // Define the API routes
    let routes = warp::path!("api" / "get_substrait_ir")
        .and(warp::get())
        .and(warp::body::bytes())
        .and_then(get_substrait_ir);

    // Start the warp server
    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}