package org.apache.flink.connectors.mongodb.utils;

import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.Map;

/**
 * @Author: kewang
 * @Date: 2022/4/18 15:47
 */
public class ContextUtil {
    public static void transformContext(DynamicTableFactory factory, DynamicTableFactory.Context context) {
        Map<String, String> catalogOptions = context.getCatalogTable().getOptions();

        Map<String, String> convertedOptions = FactoryOptionUtil.normalizeOptionCaseAsFactory(factory, catalogOptions);

        catalogOptions.clear();
        for (Map.Entry<String, String> entry : convertedOptions.entrySet()) {
            catalogOptions.put(entry.getKey(), entry.getValue());
        }
    }
}
