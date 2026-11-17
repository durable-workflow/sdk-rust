fn main() {
    let _client = durable_workflow::Client::builder("http://127.0.0.1:8080");
    let _schema = apache_avro::Schema::parse_str("\"null\"").unwrap();
}
