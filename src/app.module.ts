import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { KafkaModule } from './kakfa/kafka.module';

@Module({
  imports: [KafkaModule.forRoot()],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
