package com.ververica.flinktraining.exercises.datastream_java.process;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Задание: сгенерировать предупреждение, если поездка длится более 2 часов.
 * Выводятся только события начала поездки (START), если по ним в течение 2 часов не поступило END.
 */
public class LongRidesExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);

		final int maxEventDelay = 60;       // макс. задержка событий — 60 сек.
		final int servingSpeedFactor = 600; // 10 минут событий за 1 сек.

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env.addSource(
				rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor))
		);

		DataStream<TaxiRide> longRides = rides
				.keyBy(ride -> ride.rideId)
				.process(new LongRideAlertFunction());

		printOrTest(longRides);

		env.execute("Long Taxi Rides");
	}

	/**
	 * Сигнализирует о поездках, не завершённых через 2 часа.
	 */
	public static class LongRideAlertFunction extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {

		private transient ValueState<TaxiRide> startState;
		private transient ValueState<TaxiRide> endState;

		@Override
		public void open(Configuration parameters) throws Exception {
			startState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("start-ride", TaxiRide.class));
			endState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("end-ride", TaxiRide.class));
		}

		@Override
		public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> out) throws Exception {
			TimerService timerService = context.timerService();

			if (ride.isStart) {
				TaxiRide end = endState.value();
				if (end == null) {
					// START пришёл раньше END — сохраняем и регистрируем таймер
					startState.update(ride);
					timerService.registerEventTimeTimer(ride.getEventTime() + 2 * 60 * 60 * 1000);
				} else {
					// END уже был — очищаем
					endState.clear();
				}
			} else {
				TaxiRide start = startState.value();
				if (start != null) {
					// START уже был — всё завершено, очищаем
					startState.clear();
				} else {
					// END пришёл раньше — сохраняем его
					endState.update(ride);
				}
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {
			TaxiRide start = startState.value();
			if (start != null) {
				// прошло 2 часа, END не поступил — сигналим
				out.collect(start);
				startState.clear();
			}
		}
	}
}
