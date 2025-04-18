import { KafkaJS } from '@confluentinc/kafka-javascript';
import { Inject } from '@nestjs/common';
import { KafkaConnectionOptions } from '../interfaces/kafka-connection-options';
import {
  KAFKA_ADMIN_CLIENT_PROVIDER,
  KAFKA_CONFIGURATION_PROVIDER,
  KAFKA_CONSUMER_PROVIDER,
} from './kafka.connection';

export interface KafkaMetrics {
  lag?: number;
  consumerOffset?: number;
  producerOffset?: number;
}

export class KafkaMetricsProvider {
  constructor(
    @Inject(KAFKA_ADMIN_CLIENT_PROVIDER) private readonly admin?: KafkaJS.Admin,
    @Inject(KAFKA_CONFIGURATION_PROVIDER)
    private readonly config?: KafkaConnectionOptions,
    @Inject(KAFKA_CONSUMER_PROVIDER)
    private readonly consumer?: KafkaJS.Consumer,
  ) {}
  private checkPrerequisites(): void {
    const consumerGroupId = this.config?.consumer?.conf['group.id'];

    if (!consumerGroupId) {
      throw new Error(
        "Consumer group id not provided. Did you forget to provide 'group.id' in consumer configuration?",
      );
    }
    if (!this.admin) {
      throw new Error(
        "Admin client not provided. Did you forget to provide 'adminClient' configuration in KafkaModule?",
      );
    }
    if (!this.consumer) {
      throw new Error(
        "Consumer not provided. Did you forget to provide 'consumer' configuration in KafkaModule?",
      );
    }
  }

  async getMetrics(): Promise<Record<string, KafkaMetrics>> {
    this.checkPrerequisites();

    const consumerGroupId = this.config!.consumer!.conf['group.id']!;

    let topics: string[];
    try {
      topics = (await this.admin!.fetchTopicMetadata()).topics.map(
        (topic) => topic.name,
      );
    } catch (e) {
      topics = this.consumer?.assignment()?.map((topic) => topic.topic) ?? [];
    }

    const topicMetrics: Record<string, KafkaMetrics> = {};

    for (const topic of topics) {
      //Fetch producer offsets
      const producerOffset = await this.admin!.fetchTopicOffsets(topic);

      //Fetch consumer offsets for the consumer group
      const consumerOffsets = await this.admin!.fetchOffsets({
        groupId: consumerGroupId,
        topics: [topic],
      });

      //Find the max offset for each partition
      consumerOffsets.forEach((offset) => {
        topicMetrics[offset.topic] = {
          consumerOffset: offset.partitions.reduce(
            (prev, curr) => Math.max(prev, Number.parseInt(curr.offset)),
            0,
          ),
        };
      });

      producerOffset.forEach((offset) => {
        topicMetrics[topic].producerOffset = Number.parseInt(offset.offset);
      });
    }

    Object.keys(topicMetrics).forEach((topic) => {
      topicMetrics[topic].lag =
        (topicMetrics[topic].producerOffset ?? 0) -
        (topicMetrics[topic].consumerOffset ?? 0);
    });

    return topicMetrics;
  }
}
