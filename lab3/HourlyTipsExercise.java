package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		// Чтение параметров
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;         // Максимальная задержка событий — 60 секунд
		final int servingSpeedFactor = 600;   // Скорость воспроизведения — 10 минут данных за 1 секунду

		// Настройка среды выполнения Flink
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // Работа во временной характеристике событий
		env.setParallelism(ExerciseBase.parallelism); // Используем установленный уровень параллелизма

		// Источник данных (события такси с временными метками и watermark)
		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

		// Подсчёт суммы чаевых по каждому водителю за каждый час
		DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
				.keyBy(fare -> fare.driverId) // группировка по идентификатору водителя
				.window(TumblingEventTimeWindows.of(Time.hours(1))) // скользящие окна по 1 часу
				.process(new CalcTips()); // вычисление суммы чаевых

		// Определение водителя с максимальной суммой чаевых за каждый час
		DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
				.windowAll(TumblingEventTimeWindows.of(Time.hours(1))) // общее окно для всех водителей
				.maxBy(2); // выбираем максимальное значение по полю "сумма чаевых"

		// Печать результата или проверка в тесте
		printOrTest(hourlyMax);

		// Запуск выполнения задачи
		env.execute("Hourly Tips (java)");
	}

	/**
	 * Функция обработки окна: считает общую сумму чаевых для одного водителя за час.
	 */
	public static class CalcTips extends ProcessWindowFunction<
			TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {

		@Override
		public void process(Long driverId, Context context, Iterable<TaxiFare> fares,
							Collector<Tuple3<Long, Long, Float>> out) {
			float sum = 0;
			// Суммируем чаевые всех поездок в пределах окна
			for (TaxiFare fare : fares) {
				sum += fare.tip;
			}
			long windowEnd = context.window().getEnd(); // конец окна (timestamp)
			out.collect(new Tuple3<>(windowEnd, driverId, sum)); // отдаём результат
		}
	}
}
