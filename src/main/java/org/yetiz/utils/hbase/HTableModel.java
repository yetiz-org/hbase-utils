package org.yetiz.utils.hbase;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections.map.UnmodifiableMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yetiz.utils.hbase.exception.InvalidOperationException;
import org.yetiz.utils.hbase.exception.TypeNotFoundException;
import org.yetiz.utils.hbase.utils.ModelCallbackTask;

import javax.xml.bind.DatatypeConverter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by yeti on 16/4/1.
 */
public abstract class HTableModel<T extends HTableModel> {
	protected static final ObjectMapper JSON_MAPPER = new ObjectMapper(new JsonFactory());
	private static final HashMap<TableName, HashMap<String, Qualifier>>
		ModelQualifiers = new HashMap<>();
	private static final HashMap<TableName, HashMap<String, Family>>
		ModelFamilies = new HashMap<>();
	private static final HashMap<Class<? extends HTableModel>, TableName>
		ModelTableNameMaps = new HashMap<>();
	private static final HashMap<TableName, Class<? extends HTableModel>>
		TableNameModelMaps = new HashMap<>();
	private static final HashMap<TableName, HashMap<String, String>>
		ModelFQFields = new HashMap<>();
	private static final Reflections REFLECTION = new Reflections("");
	private static final ValueSetterPackage DEFAULT_VALUE_SETTER_PACKAGE = new ValueSetterPackage("", "", null);
	private static Field resultField;
	private static Field isResultField;
	private static Logger LOGGER = LoggerFactory.getLogger(HTableModel.class);

	static {
		initModelQualifier();
		try {
			resultField = HTableModel.class.getDeclaredField("result");
			isResultField = HTableModel.class.getDeclaredField("isResult");
		} catch (Throwable throwable) {
		}
	}

	private final HashMap<String, ValueSetterPackage> setValues;
	private boolean isResult;
	private Result result = null;
	private boolean copy = false;

	public HTableModel() {
		isResult = false;
		setValues = new HashMap<>();
	}

	/**
	 * get <code>Qualifier</code> by method name, this is readonly map
	 *
	 * @param tableName
	 * @return
	 */
	public static final Map<String, Qualifier> qualifiers(TableName tableName) {
		return UnmodifiableMap.decorate(ModelQualifiers.get(tableName));
	}

	/**
	 * get <code>Family</code> by method name, this is readonly map
	 *
	 * @param tableName
	 * @return
	 */
	public static final Map<String, Family> families(TableName tableName) {
		return UnmodifiableMap.decorate(ModelFamilies.get(tableName));
	}

	public static final TableName tableName(Class<? extends HTableModel> type) {
		return ModelTableNameMaps.get(type);
	}

	public static final Class<? extends HTableModel> modelType(TableName tableName) {
		return TableNameModelMaps.get(tableName);
	}

	public static final <R extends HTableModel> R newWrappedModel(TableName tableName, Result result) {
		try {
			R r = (R) TableNameModelMaps.get(tableName).newInstance();
			resultField.set(r, result);
			isResultField.set(r, true);
			return r;
		} catch (Throwable throwable) {
			throw new TypeNotFoundException(throwable);
		}
	}

	public static final void DBDrop(HBaseClient client) {
		for (Class<? extends HTableModel> type : implementedModels()) {
			try {
				type.newInstance().drop(client);
			} catch (Throwable throwable) {
			}
		}
	}

	private static final List<Class<? extends HTableModel>> implementedModels() {
		Set<Class<? extends HTableModel>> reflects = REFLECTION.getSubTypesOf(HTableModel.class);
		List<Class<? extends HTableModel>> abstracts = new ArrayList<>();
		for (Class<? extends HTableModel> type : reflects) {
			if (Modifier.isAbstract(type.getModifiers())) {
				abstracts.add(type);
			}
		}

		reflects.removeAll(abstracts);
		return new ArrayList<>(reflects);
	}

	public void drop(HBaseClient client) {
		client.admin().deleteTable(tableName());
	}

	public TableName tableName() {
		return ModelTableNameMaps.get(this.getClass());
	}

	public static final void DBMigration(HBaseClient client) {
		LOGGER.info("Start migration.");
		for (Class<? extends HTableModel> type : implementedModels()) {
			try {
				type.newInstance().migrate(client);
			} catch (Throwable throwable) {
			}
		}
		LOGGER.info("Migration done.");
	}

	public void migrate(HBaseClient client) {
		if (!client.admin().tableExists(tableName())) {
			client.admin().createTable(tableName());
		}

		LOGGER.info(String.format("%s migrating...", tableName().get().getNameAsString()));

		ObjectNode root = JSON_MAPPER.createObjectNode();
		root.put("object_name", this.getClass().getName());
		HTableDescriptor descriptor = client.admin().tableDescriptor(tableName());

		ArrayNode families = JSON_MAPPER.createArrayNode();
		HTableDescriptor finalDescriptor = descriptor;
		HashMap<String, ArrayNode> familyMaps = new HashMap<>();
		HashMap<String, String> familyComp = new HashMap<>();

		for (Map.Entry<String, Family> entry : ModelFamilies.get(tableName()).entrySet()) {
			ArrayNode nodes = familyMaps.get(entry.getValue().family());
			if (nodes == null) {
				nodes = JSON_MAPPER.createArrayNode();
			}

			familyMaps.put(entry.getValue().family(), nodes.add(JSON_MAPPER.createObjectNode()
				.put("field_name", entry.getKey())
				.put("qualifier", ModelQualifiers.get(tableName()).get(entry.getKey()).qualifier())
				.put("description", ModelQualifiers.get(tableName()).get(entry.getKey()).description())));
			familyComp.put(entry.getValue().family(), entry.getValue().compression().getName());
		}

		HashMap<String, Family> collect = new HashMap<>();

		for (Map.Entry<String, Family> entry : ModelFamilies.get(tableName()).entrySet()) {
			if (!collect.containsKey(entry.getValue().family())) {
				collect.put(entry.getValue().family(), entry.getValue());
			}
		}

		for (Map.Entry<String, Family> entry : collect.entrySet()) {
			if (!finalDescriptor.hasFamily(HBaseClient.bytes(entry.getKey()))) {
				client
					.admin()
					.addColumnFamily(tableName(),
						entry.getValue().family(),
						entry.getValue().compression());
			}
		}

		for (Map.Entry<String, ArrayNode> entry : familyMaps.entrySet()) {
			families
				.add(JSON_MAPPER.createObjectNode()
					.put("family", entry.getKey())
					.put("compression", familyComp.get(entry.getKey()))
					.set("qualifiers", entry.getValue()));
		}

		root.set("families", families);
		descriptor = client.admin().tableDescriptor(tableName());
		descriptor.setValue("description".getBytes(), root.toString().getBytes());
		client.admin().updateTable(tableName(), descriptor);
	}

	private static void initModelQualifier() {
		for (Class<? extends HTableModel> type : implementedModels()) {
			try {
				TableName tableName = TableName.valueOf(type.getSimpleName());
				ModelTableNameMaps.put(type, tableName);
				TableNameModelMaps.put(tableName, type);
			} catch (Throwable throwable) {
			}
		}

		for (Class<? extends HTableModel> type : implementedModels()) {
			try {
				try {
					TableName tableName = type.newInstance().tableName();
					List<Method> methods = methods(type, null);

					HashMap<String, String> fqFields = new HashMap<>();
					for (Method method : methods) {
						if (method.getAnnotation(Family.class) != null &&
							method.getAnnotation(Qualifier.class) != null) {
							fqFields.put(method.getAnnotation(Family.class).family() +
									"+-" +
									method.getAnnotation(Qualifier.class).qualifier(),
								method.getName());
						}
					}

					ModelFQFields.put(tableName, fqFields);

					HashMap<String, Qualifier> mqs = new HashMap<>();
					for (Method method : methods) {
						if (method.getAnnotation(Qualifier.class) != null) {
							mqs.put(method.getName(), method.getAnnotation(Qualifier.class));
						}
					}

					ModelQualifiers.put(tableName, mqs);

					HashMap<String, Family> mfs = new HashMap<>();
					for (Method method : methods) {
						if (method.getAnnotation(Family.class) != null) {
							mfs.put(method.getName(), method.getAnnotation(Family.class));
						}
					}

					ModelFamilies.put(tableName, mfs);
				} catch (Throwable throwable) {
				}
			} catch (Throwable throwable) {
			}
		}
	}

	private static final List<Field> fields(Class type, List<Field> fields) {
		if (fields == null) {
			fields = new ArrayList<>();
		}

		fields.addAll(Arrays.asList(type.getDeclaredFields()));

		if (type.getSuperclass() != null) {
			fields = fields(type.getSuperclass(), fields);
		}

		return fields;
	}

	private static final List<Method> methods(Class type, List<Method> methods) {
		if (methods == null) {
			methods = new ArrayList<>();
		}

		methods.addAll(Arrays.asList(type.getDeclaredMethods()));

		if (type.getSuperclass() != null) {
			methods = methods(type.getSuperclass(), methods);
		}

		return methods;
	}

	public static Get get(byte[] row) {
		Get get = new Get(row);
		return get;
	}

	public static Delete delete(byte[] row) {
		Delete delete = new Delete(row);
		return delete;
	}

	public static final byte[] byteValueFromHex(String hex) {
		return DatatypeConverter.parseHexBinary(hex);
	}

	public static final String hexValue(byte[] value) {
		return DatatypeConverter.printHexBinary(value);
	}

	public T put(HBaseClient client) {
		if (!isResult) {
			throw new InvalidOperationException("this is not result instance.");
		}

		Put put = put(result.getRow());
		HBaseTable table = client.table(tableName());
		table.put(put);
		table.close();
		return (T) this;
	}

	public Put put(byte[] row) {
		Put put = new Put(row);
		if (!setValues.containsKey("row_updated_time")) {
			row_updated_time(System.currentTimeMillis());
		}

		for (ValueSetterPackage pack : setValues.values()) {
			put.add(byteValue(pack.family), byteValue(pack.qualifier), pack.value);
		}

		return put;
	}

	@Family(family = "d")
	@Qualifier(qualifier = "rowudt", description = "row-updated-time")
	public T row_updated_time(long updated_time) {
		return setValue(updated_time);
	}

	protected final T setValue(long longValue) {
		copyResultToSetter();

		String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
		this.setValues.put(methodName,
			new ValueSetterPackage(family(methodName), qualifier(methodName), byteValue(longValue)));
		return (T) this;
	}

	private void copyResultToSetter() {
		if (!isResult) {
			return;
		}

		if (!copy) {
			copy = true;
		} else {
			return;
		}

		for (Cell cell : result.listCells()) {
			String family = stringValue(CellUtil.cloneFamily(cell));
			String qualifier = stringValue(CellUtil.cloneQualifier(cell));
			String methodName = ModelFQFields.get(tableName())
				.get(family + "+-" + qualifier);
			if (methodName != null) {
				setValues.put(methodName, new ValueSetterPackage(family, qualifier, CellUtil.cloneValue(cell)));
			}
		}
	}

	public static final String stringValue(byte[] bytes) {
		return new String(bytes, HBaseClient.DEFAULT_CHARSET);
	}

	private final String family(String methodName) {
		return ModelFamilies.get(tableName()).get(methodName).family();
	}

	private final String qualifier(String methodName) {
		return ModelQualifiers.get(tableName()).get(methodName).qualifier();
	}

	public static final byte[] byteValue(Object object) {
		if (object instanceof CharSequence) {
			return object.toString().getBytes(HBaseClient.DEFAULT_CHARSET);
		}

		if (object instanceof String) {
			return ((String) object).getBytes(HBaseClient.DEFAULT_CHARSET);
		}

		if (object instanceof Long) {
			return ByteBuffer.allocate(8).putLong((Long) object).array();
		}

		if (object instanceof Integer) {
			return ByteBuffer.allocate(4).putInt((Integer) object).array();
		}

		if (object instanceof Float) {
			return ByteBuffer.allocate(4).putFloat((Float) object).array();
		}

		if (object instanceof Double) {
			return ByteBuffer.allocate(8).putDouble((Double) object).array();
		}

		return object.toString().getBytes(HBaseClient.DEFAULT_CHARSET);
	}

	/**
	 * When this instance is Operation result and the result is Empty, then do
	 *
	 * @param task callback task
	 * @return
	 */
	public T orThen(ModelCallbackTask<T> task) {
		if (isEmpty() && isResult) {
			task.callback((T) this);
		}
		return (T) this;
	}

	public boolean isEmpty() {
		return result == null || result.isEmpty();
	}

	/**
	 * do after previous operation
	 *
	 * @param task
	 * @return
	 */
	public T then(ModelCallbackTask<T> task) {
		task.callback((T) this);
		return (T) this;
	}

	public T delete(HBaseClient client) {
		if (!isResult) {
			throw new InvalidOperationException("this is not result instance.");
		}

		HBaseTable table = client.table(tableName());
		table.delete(new Delete(result.getRow()));
		table.close();
		return (T) this;
	}

	public Delete delete() {
		if (!isResult) {
			throw new InvalidOperationException("this is not result instance.");
		}

		return new Delete(result.getRow());
	}

	public final byte[] row() {
		return result.getRow();
	}

	protected final T setValue(String string) {
		copyResultToSetter();

		String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
		this.setValues.put(methodName,
			new ValueSetterPackage(family(methodName), qualifier(methodName), byteValue(string)));
		return (T) this;
	}

	public Long row_updated_time() {
		return longValue(retrieveValue());
	}

	public static final Long longValue(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getLong();
	}

	protected final byte[] retrieveValue() {
		String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
		if (isResult && !copy) {
			return result.getValue(HBaseClient.bytes(family(methodName)), HBaseClient.bytes(qualifier(methodName)));
		}

		ValueSetterPackage valueSetterPackage = setValues.get(methodName);
		if (valueSetterPackage == null) {
			valueSetterPackage = DEFAULT_VALUE_SETTER_PACKAGE;
		}

		return valueSetterPackage.value;
	}

	private static class ValueSetterPackage {
		public String family;
		public String qualifier;
		public byte[] value;

		public ValueSetterPackage(String family, String qualifier, byte[] value) {
			this.family = family;
			this.qualifier = qualifier;
			this.value = value;
		}
	}
}
