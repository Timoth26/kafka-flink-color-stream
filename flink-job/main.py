import json

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


class JSONTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        data = json.loads(value)
        return int(data["timestamp"])


class ColorAggregator(KeyedProcessFunction):
    def __init__(self):
        self.cumulative_state = None
        self.window_state = None
        self.window_size_ms = 10000  # 10 seconds

    def open(self, runtime_context: RuntimeContext):
        self.cumulative_state = runtime_context.get_state(
            ValueStateDescriptor("cumulative", Types.LONG())
        )
        self.window_state = runtime_context.get_state(
            ValueStateDescriptor("window", Types.LONG())
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        pass

    def on_timer(self, timestamp, ctx: "KeyedProcessFunction.OnTimerContext"):
        pass


def extract_key(value):
    return json.loads(value)["color"]


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:29092")
        .set_topics("input-color-events")
        .set_group_id("flink-python-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(2)
    ).with_timestamp_assigner(JSONTimestampAssigner())

    stream = env.from_source(source, watermark_strategy, "Kafka Source")

    processed_stream = stream.key_by(extract_key, key_type=Types.STRING()).process(
        ColorAggregator(), output_type=Types.STRING()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:29092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("output-color-stats")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    processed_stream.sink_to(sink)

    env.execute("PyFlink Color Stats Job")


if __name__ == "__main__":
    main()
