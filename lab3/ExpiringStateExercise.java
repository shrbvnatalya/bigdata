package com.ververica.flinktraining.exercises.datastream_java.process;

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
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * ExpiringStateExercise:
 * Задача — сопоставить поездки (TaxiRide) с тарифами (TaxiFare) по rideId.
 * Если соответствие не найдено в течение времени события, данные считаются "несопоставленными".
 */
public class ExpiringStateExercise extends ExerciseBase {

	// Боковой выход для поездок, к которым не найден тариф
	static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};

	// Боковой выход для тарифов, к которым не найдена поездка
	static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

	public static void main(String[] args) throws Exception {
		// Чтение параметров запуска
		ParameterTool params = ParameterTool.fromArgs(args);
		final String ridesFile = params.get("rides", ExerciseBase.pathToRideData);
		final String faresFile = params.get("fares", ExerciseBase.pathToFareData);

		// Параметры задержки событий и скорости проигрывания
		final int maxEventDelay = 60; // секунды
		final int servingSpeedFactor = 600; // ускорение по времени

		// Инициализация среды выполнения Flink
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// Чтение и подготовка потока поездок
		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))
				// Отфильтровываются только начальные события поездок, кроме rideId кратных 1000
				.filter(ride -> ride.isStart && (ride.rideId % 1000 != 0))
				// Группировка по rideId
				.keyBy(ride -> ride.rideId);

		// Чтение и подготовка потока тарифов
		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor)))
				// Группировка по rideId
				.keyBy(fare -> fare.rideId);

		// Сопоставление поездок и тарифов
		SingleOutputStreamOperator<Tuple2<TaxiRide, TaxiFare>> matched = rides
				.connect(fares)
				.process(new EnrichmentFunction());

		// Печать несопоставленных тарифов (поездки также можно печатать при необходимости)
		printOrTest(matched.getSideOutput(unmatchedFares));

		// Запуск Flink-приложения
		env.execute("ExpiringStateSolution (java)");
	}

	/**
	 * EnrichmentFunction:
	 * Сохраняет полученные поездки и тарифы во временное состояние.
	 * Если приходит соответствующая пара — она выводится в основной поток.
	 * Если пара не найдена до времени события — отправляется в боковой выход.
	 */
	public static class EnrichmentFunction extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

		private ValueState<TaxiRide> rideState;  // временное хранение поездки
		private ValueState<TaxiFare> fareState;  // временное хранение тарифа

		@Override
		public void open(Configuration config) {
			// Инициализация состояний Flink
			rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved-ride", TaxiRide.class));
			fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved-fare", TaxiFare.class));
		}

		// Обработка элемента TaxiRide
		@Override
		public void processElement1(TaxiRide ride, Context ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiFare fare = fareState.value();
			if (fare != null) {
				// Если тариф уже есть — выводим пару и очищаем состояния
				fareState.clear();
				ctx.timerService().deleteEventTimeTimer(fare.getEventTime());
				out.collect(new Tuple2<>(ride, fare));
			} else {
				// Иначе сохраняем поездку и устанавливаем таймер
				rideState.update(ride);
				ctx.timerService().registerEventTimeTimer(ride.getEventTime());
			}
		}

		// Обработка элемента TaxiFare
		@Override
		public void processElement2(TaxiFare fare, Context ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiRide ride = rideState.value();
			if (ride != null) {
				// Если поездка уже есть — выводим пару и очищаем состояния
				rideState.clear();
				ctx.timerService().deleteEventTimeTimer(ride.getEventTime());
				out.collect(new Tuple2<>(ride, fare));
			} else {
				// Иначе сохраняем тариф и устанавливаем таймер
				fareState.update(fare);
				ctx.timerService().registerEventTimeTimer(fare.getEventTime());
			}
		}

		// Вызов таймера — очищаем несопоставленные записи
		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			if (rideState.value() != null) {
				ctx.output(unmatchedRides, rideState.value());
				rideState.clear();
			}
			if (fareState.value() != null) {
				ctx.output(unmatchedFares, fareState.value());
				fareState.clear();
			}
		}
	}
}
