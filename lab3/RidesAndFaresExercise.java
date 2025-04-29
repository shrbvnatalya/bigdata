package com.ververica.flinktraining.exercises.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Упражнение: объединение данных поездок и стоимости поездок (enrichment).
 * Цель: объединить события начала поездки с соответствующей информацией о стоимости.
 *
 * Параметры:
 * -rides путь-к-файлу-с-поездками
 * -fares путь-к-файлу-с-стоимостью
 */
public class RidesAndFaresExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {
		// Получение путей к файлам
		ParameterTool params = ParameterTool.fromArgs(args);
		final String rideDataPath = params.get("rides", pathToRideData);
		final String fareDataPath = params.get("fares", pathToFareData);

		// Параметры задержки и скорости подачи событий
		final int maxEventDelay = 60;
		final int speedFactor = 1800;

		// Настройка среды выполнения Flink
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		// Источник данных поездок (только события начала поездки)
		DataStream<TaxiRide> rideStream = env
				.addSource(rideSourceOrTest(new TaxiRideSource(rideDataPath, maxEventDelay, speedFactor)))
				.filter(ride -> ride.isStart) // Правильное обращение к полю
				.keyBy("rideId");

		// Источник данных стоимости
		DataStream<TaxiFare> fareStream = env
				.addSource(fareSourceOrTest(new TaxiFareSource(fareDataPath, maxEventDelay, speedFactor)))
				.keyBy("rideId");

		// Объединение потоков и обогащение информации
		DataStream<Tuple2<TaxiRide, TaxiFare>> joinedStream = rideStream
				.connect(fareStream)
				.flatMap(new EnrichmentFunction());

		// Вывод результата
		printOrTest(joinedStream);

		// Запуск Flink-приложения
		env.execute("Taxi Rides & Fares Enrichment");
	}

	/**
	 * Функция для объединения информации о поездке и стоимости.
	 * Использует ValueState для хранения неполных данных до момента объединения.
	 */
	public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

		private transient ValueState<TaxiRide> pendingRide;
		private transient ValueState<TaxiFare> pendingFare;

		@Override
		public void open(Configuration config) {
			pendingRide = getRuntimeContext().getState(new ValueStateDescriptor<>("ride", TaxiRide.class));
			pendingFare = getRuntimeContext().getState(new ValueStateDescriptor<>("fare", TaxiFare.class));
		}

		@Override
		public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiFare fare = pendingFare.value();
			if (fare != null) {
				pendingFare.clear();
				out.collect(Tuple2.of(ride, fare));
			} else {
				pendingRide.update(ride);
			}
		}

		@Override
		public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiRide ride = pendingRide.value();
			if (ride != null) {
				pendingRide.clear();
				out.collect(Tuple2.of(ride, fare));
			} else {
				pendingFare.update(fare);
			}
		}
	}
}
