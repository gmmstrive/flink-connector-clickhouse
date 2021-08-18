package pl.touk.flink.ignite;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.module.ModuleManager;

import java.lang.reflect.Method;
import java.util.Map;

/*
  Workaround for StreamTableEnvironmentImpl.create check :
  		if (!settings.isStreamingMode()) {
			throw new TableException(
				"StreamTableEnvironment can not run in batch mode for now, please use TableEnvironment.");
		}
  http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/Conversion-of-Table-Blink-batch-to-DataStream-tc34080.html#a34090
 */
public class StreamTableEnvironmentUtil {

    public static StreamTableEnvironment create(
            StreamExecutionEnvironment executionEnvironment,
            EnvironmentSettings settings) {

        // temporary solution until FLINK-15635 is fixed
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        ModuleManager moduleManager = new ModuleManager();

        CatalogManager catalogManager = CatalogManager.newBuilder()
                .classLoader(classLoader)
                .config(new Configuration())
                .defaultCatalog(
                        settings.getBuiltInCatalogName(),
                        new GenericInMemoryCatalog(
                                settings.getBuiltInCatalogName(),
                                settings.getBuiltInDatabaseName()))
                .executionConfig(executionEnvironment.getConfig())
                .build();

        TableConfig tableConfig = new TableConfig();
        tableConfig.getConfiguration().set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        FunctionCatalog functionCatalog = new FunctionCatalog(tableConfig, catalogManager, moduleManager);

        Map<String, String> executorProperties = settings.toExecutorProperties();
        Executor executor = lookupExecutor(executorProperties, executionEnvironment);

        Map<String, String> plannerProperties = settings.toPlannerProperties();
        Planner planner = ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
                .create(plannerProperties, executor, tableConfig, functionCatalog, catalogManager);

        return new StreamTableEnvironmentImpl(
                catalogManager,
                moduleManager,
                functionCatalog,
                tableConfig,
                executionEnvironment,
                planner,
                executor,
                settings.isStreamingMode(),
                classLoader
        );
    }

    private static Executor lookupExecutor(
            Map<String, String> executorProperties,
            StreamExecutionEnvironment executionEnvironment) {
        try {
            ExecutorFactory executorFactory = ComponentFactoryService.find(ExecutorFactory.class, executorProperties);
            Method createMethod = executorFactory.getClass()
                    .getMethod("create", Map.class, StreamExecutionEnvironment.class);

            return (Executor) createMethod.invoke(
                    executorFactory,
                    executorProperties,
                    executionEnvironment);
        } catch (Exception e) {
            throw new TableException(
                    "Could not instantiate the executor. Make sure a planner module is on the classpath",
                    e);
        }
    }

}
