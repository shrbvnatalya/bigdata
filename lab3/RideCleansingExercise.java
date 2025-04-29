package com.ververica.flinktraining.exercises.datastream_java.basics;

import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Очистка данных: оставить только те поездки, которые начинаются и заканчиваются в пределах Нью-Йорка.
 */
public class RideCleansingExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		// Получаем путь к файлу с данными
		ParameterTool parameters = ParameterTool.fromArgs(args);
		final String rideDataPath = parameters.get("input", ExerciseBase.pathToRideData);

		// Параметры генерации событий
		final int maxDelaySeconds = 60;        // Максимальная задержка событий
		final int speedFactor = 600;           // Ускорение подачи данных

		// Настройка среды выполнения Flink
		StreamExecutionEnvironment executionEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		executionEnv.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);

		// Создание источника потоковых данных
		DataStream<TaxiRide> rideStream = executionEnv.addSource(
				rideSourceOrTest(new TaxiRideSource(rideDataPath, maxDelaySeconds, speedFactor)));

		// Применение фильтрации: только поездки в пределах NYC
		DataStream<TaxiRide> nycOnlyRides = rideStream
				.filter(new InNYCFilter());

		// Печать отфильтрованных поездок или выполнение теста
		printOrTest(nycOnlyRides);

		// Запуск Flink-пайплайна
		executionEnv.execute("Ride Cleansing: NYC Only");
	}

	/**
	 * Пользовательская функция фильтрации для отбора поездок в пределах NYC.
	 */
	public static class InNYCFilter implements FilterFunction<TaxiRide> {
		@Override
		public boolean filter(TaxiRide ride) {
			return GeoUtils.isInNYC(ride.startLon, ride.startLat)
					&& GeoUtils.isInNYC(ride.endLon, ride.endLat);
		}
	}
}
