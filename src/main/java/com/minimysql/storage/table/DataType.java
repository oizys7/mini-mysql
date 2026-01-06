package com.minimysql.storage.table;

/**
 * DataType - 数据类型枚举
 *
 * 定义Mini MySQL支持的数据类型。
 *
 * 类型分类:
 * - 整数类型: INT, BIGINT
 * - 浮点类型: DOUBLE
 * - 布尔类型: BOOLEAN
 * - 字符串类型: VARCHAR
 * - 日期时间: DATE, TIMESTAMP
 *
 * 设计原则:
 * - 简化MySQL的类型系统,保留最常用的类型
 * - 每种类型有固定的存储格式
 * - 支持NULL值(在Row中通过位图实现)
 *
 * 存储格式:
 * - INT: 4字节,小端序
 * - BIGINT: 8字节,小端序
 * - DOUBLE: 8字节,IEEE 754
 * - BOOLEAN: 1字节,0=false, 1=true
 * - VARCHAR(n): 2字节长度 + n字节数据(UTF-8)
 * - DATE: 8字节,时间戳(毫秒)
 * - TIMESTAMP: 8字节,时间戳(毫秒)
 *
 * "Good taste": 每种类型的存储格式固定,没有特殊情况
 */
public enum DataType {

    /**
     * 整数类型: 4字节,范围 -2^31 到 2^31-1
     */
    INT(4, false, Integer.class),

    /**
     * 长整数类型: 8字节,范围 -2^63 到 2^63-1
     */
    BIGINT(8, false, Long.class),

    /**
     * 双精度浮点: 8字节,IEEE 754
     */
    DOUBLE(8, false, Double.class),

    /**
     * 布尔类型: 1字节,0=false, 非0=true
     */
    BOOLEAN(1, false, Boolean.class),

    /**
     * 变长字符串: 最大长度由参数指定
     * 存储: 2字节长度前缀 + 实际数据
     */
    VARCHAR(-1, true, String.class),

    /**
     * 日期类型: 存储为时间戳(毫秒)
     */
    DATE(8, false, java.util.Date.class),

    /**
     * 时间戳类型: 精确到毫秒
     */
    TIMESTAMP(8, false, java.util.Date.class);

    /** 固定存储大小(字节),-1表示变长 */
    private final int storageSize;

    /** 是否为变长类型 */
    private final boolean variableLength;

    /** 对应的Java类型 */
    private final Class<?> javaType;

    /**
     * 构造数据类型
     *
     * @param storageSize 固定存储大小(字节),-1表示变长
     * @param variableLength 是否为变长类型
     * @param javaType 对应的Java类型
     */
    DataType(int storageSize, boolean variableLength, Class<?> javaType) {
        this.storageSize = storageSize;
        this.variableLength = variableLength;
        this.javaType = javaType;
    }

    /**
     * 获取固定存储大小
     *
     * @return 存储大小(字节),如果是变长类型返回-1
     */
    public int getStorageSize() {
        return storageSize;
    }

    /**
     * 是否为变长类型
     *
     * @return 如果是变长类型返回true
     */
    public boolean isVariableLength() {
        return variableLength;
    }

    /**
     * 获取对应的Java类型
     *
     * @return Java类型
     */
    public Class<?> getJavaType() {
        return javaType;
    }

    /**
     * 验证值是否符合类型约束
     *
     * @param value 要验证的值
     * @param typeLength 类型长度(仅VARCHAR有效)
     * @return 如果值有效返回true
     */
    public boolean validate(Object value, int typeLength) {
        if (value == null) {
            return true; // NULL值在Row层面处理
        }

        // 检查Java类型
        if (!javaType.isInstance(value)) {
            return false;
        }

        // VARCHAR特殊检查长度
        if (this == VARCHAR) {
            String str = (String) value;
            return str.length() <= typeLength;
        }

        return true;
    }

    /**
     * 根据名称解析数据类型
     *
     * @param typeName 类型名称(如"INT", "VARCHAR")
     * @return 数据类型,如果未知返回null
     */
    public static DataType fromName(String typeName) {
        try {
            return valueOf(typeName.toUpperCase());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
