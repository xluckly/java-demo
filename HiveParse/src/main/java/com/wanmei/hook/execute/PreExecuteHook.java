package com.wanmei.hook.execute;


import com.google.common.collect.Lists;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.gson.stream.JsonWriter;
import com.wanmei.hook.common.KafkaUtil;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.hooks.*;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.Utilities;

import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.tools.LineageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;



/**
 * @author hanqingkuo@pwrd.com
 * @date 2020/7/8 15:23
 */
public class PreExecuteHook  implements ExecuteWithHookContext {
    private static final Logger LOG = LoggerFactory.getLogger(PreExecuteHook.class);
    private static final HashSet<String> OPERATION_NAMES = new HashSet<String>();

    static {
        OPERATION_NAMES.add(HiveOperation.QUERY.getOperationName());
        OPERATION_NAMES.add(HiveOperation.CREATETABLE_AS_SELECT.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERVIEW_AS.getOperationName());
        OPERATION_NAMES.add(HiveOperation.CREATEVIEW.getOperationName());
    }

    private static final String FORMAT_VERSION = "1.0";

    public static final class Edge {

        /**
         * The types of Edge.
         */
        public static enum Type {
            PROJECTION, PREDICATE
        }

        private Set<Vertex> sources;
        private Set<Vertex> targets;
        private String expr;
        private Edge.Type type;

        Edge(Set<Vertex> sources, Set<Vertex> targets, String expr, Edge.Type type) {
            this.sources = sources;
            this.targets = targets;
            this.expr = expr;
            this.type = type;
        }
    }

    public static final class Vertex {

        /**
         * A type in lineage.
         */
        public static enum Type {
            COLUMN, TABLE
        }

        private Type type;
        private String label;
        private int id;

        Vertex(String label) {
            this(label, Type.COLUMN);
        }

        Vertex(String label, Type type) {
            this.label = label;
            this.type = type;
        }

        @Override
        public int hashCode() {
            return label.hashCode() + type.hashCode() * 3;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Vertex)) {
                return false;
            }
            Vertex vertex = (Vertex) obj;
            return label.equals(vertex.label) && type == vertex.type;
        }


        public Type getType() {
            return type;
        }


        public String getLabel() {
            return label;
        }


        public int getId() {
            return id;
        }
    }

    public void getTableLineage(JsonWriter writer, QueryPlan plan) {
        String queryStr = plan.getQueryStr().trim();  //sql
        if (queryStr == null) {
            LOG.info("queryStr is null");
            System.exit(1);
        }
        try {
            LineageInfo lineageInfo = new org.apache.hadoop.hive.ql.tools.LineageInfo();
            lineageInfo.getLineageInfo(queryStr);
            TreeSet<String> tableInput = lineageInfo.getInputTableList();
            TreeSet<String> tableOutput = lineageInfo.getOutputTableList();

            if (tableInput == null) {
                LOG.info("input table is null");

            } else {
                writer.name("input");
                writer.beginArray();
                for (String tab : tableInput) {
                    writer.value(tab);
                    LOG.info("input: " + tab);
                }
                writer.endArray();
            }
            if (tableOutput == null) {
                LOG.info("input table is null");

            } else {
                writer.name("output");
                writer.beginArray();
                for (String tab : tableOutput) {
                    LOG.info("output: " + tab);
                    writer.value(tab);
                }
                writer.endArray();
            }


        } catch (Exception e) {
            LOG.error("Exection " + e);
        }
    }
    @Override
    public void run(HookContext hookContext) throws Exception {
        LOG.info("check HookType.PRE_EXEC_HOOK");
        assert (hookContext.getHookType() == HookContext.HookType.PRE_EXEC_HOOK);
        LOG.info("assert success HookType.PRE_EXEC_HOOK");

        SessionState ss = SessionState.get();
        LineageCtx.Index index = hookContext.getIndex();

        QueryPlan plan = hookContext.getQueryPlan();

        if (ss != null && index != null
            && OPERATION_NAMES.contains(plan.getOperationName())
            && !plan.isExplain()) {
            try {
                StringBuilderWriter out = new StringBuilderWriter(1024);
                JsonWriter writer = new JsonWriter(out);

                String queryStr = plan.getQueryStr().trim();
                writer.beginObject();
                getTableLineage(writer, plan);
                writer.name("version").value(FORMAT_VERSION);
                HiveConf conf = ss.getConf();
                boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST);
                if (!testMode) {
                    // Don't emit user/timestamp info in test mode,
                    // so that the test golden output file is fixed.
                    long queryTime = plan.getQueryStartTime().longValue();
                    if (queryTime == 0) {
                        queryTime = System.currentTimeMillis();
                    }
                    long duration = System.currentTimeMillis() - queryTime;
                    writer.name("user").value(hookContext.getUgi().getUserName());
                    writer.name("timestamp").value(queryTime / 1000);
                    writer.name("duration").value(duration);
                    writer.name("jobIds");
                    writer.beginArray();
                    List<TaskRunner> taskRunners = hookContext.getCompleteTaskList();
                    if (taskRunners == null && !taskRunners.isEmpty()) {
                        for (TaskRunner taskRunner : taskRunners) {
                            String id = taskRunner.getTask().getId();
                            if (id != null) {
                                writer.value(taskRunner.getName());
                            }
                        }
                    }
                    writer.endArray();
                }
                writer.name("engine").value(
                        HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE));
                writer.name("database").value(ss.getCurrentDatabase());
                writer.name("hash").value(getQueryHash(queryStr));
                writer.name("queryText").value(queryStr);
                List<Edge> edges = getEdges(plan, index);
                Set<Vertex> vertices = getVertices(edges);
                writeEdges(writer, edges, hookContext.getConf());
                writeVertices(writer, vertices);
                writer.endObject();
                writer.close();

                String lineage = out.toString();
                if (testMode) {
                    // Logger to console
                    log(lineage);
                } else {
                    // In non-test mode, emit to a log file,
                    // which can be different from the normal hive.log.
                    // For example, using NoDeleteRollingFileAppender to
                    // log to some file with different rolling policy.
                    LOG.info(lineage);
                    KafkaUtil.writeTokafka(lineage);
                }
            } catch (Exception e) {
                LOG.error("Failed to log lineage graph, query is not affected: " + e.toString());
            }
        }
    }

    private void writeVertices(JsonWriter writer, Set<Vertex> vertices) throws IOException {
        writer.name("vertices");
        writer.beginArray();
        for (Vertex vertex: vertices) {
            writer.beginObject();
            writer.name("id").value(vertex.id);
            writer.name("vertexType").value(vertex.type.name());
            writer.name("vertexId").value(vertex.label);
            writer.endObject();
        }
        writer.endArray();
    }

    private void writeEdges(JsonWriter writer, List<Edge> edges, HiveConf conf)
            throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        writer.name("edges");
        writer.beginArray();
        for (Edge edge: edges) {
            writer.beginObject();
            writer.name("sources");
            writer.beginArray();
            for (Vertex vertex: edge.sources) {
                writer.value(vertex.id);
            }
            writer.endArray();
            writer.name("targets");
            writer.beginArray();
            for (Vertex vertex: edge.targets) {
                writer.value(vertex.id);
            }
            writer.endArray();
            if (edge.expr != null) {
                writer.name("expression").value(HookUtils.redactLogString(conf, edge.expr));
            }
            writer.name("edgeType").value(edge.type.name());
            writer.endObject();
        }
        writer.endArray();
    }
    public static Set<Vertex> getVertices(List<Edge> edges) {
        Set<Vertex> vertices = new LinkedHashSet<Vertex>();
        for (Edge edge: edges) {
            vertices.addAll(edge.targets);
        }
        for (Edge edge: edges) {
            vertices.addAll(edge.sources);
        }

        // Assign ids to all vertices,
        // targets at first, then sources.
        int id = 0;
        for (Vertex vertex: vertices) {
            vertex.id = id++;
        }
        return vertices;
    }

    public static List<Edge> getEdges(QueryPlan plan, LineageCtx.Index index) {
        LinkedHashMap<String, ObjectPair<SelectOperator,
                org.apache.hadoop.hive.ql.metadata.Table>> finalSelOps = index.getFinalSelectOps();
        Map<String, Vertex> vertexCache = new LinkedHashMap<String, Vertex>();
        List<Edge> edges = new ArrayList<Edge>();
        for (ObjectPair<SelectOperator,
                org.apache.hadoop.hive.ql.metadata.Table> pair : finalSelOps.values()) {
            List<FieldSchema> fieldSchemas = plan.getResultSchema().getFieldSchemas();
            SelectOperator finalSelOp = pair.getFirst();
            org.apache.hadoop.hive.ql.metadata.Table t = pair.getSecond();
            String destTableName = null;
            List<String> colNames = null;
            if (t != null) {

                destTableName = t.getDbName() + "." + t.getTableName();
                fieldSchemas = t.getCols();
            } else {
                for (WriteEntity output : plan.getOutputs()) {
                    Entity.Type entityType = output.getType();
                    if (entityType == Entity.Type.TABLE
                            || entityType == Entity.Type.PARTITION) {
                        t = output.getTable();
                        destTableName = t.getDbName() + "." + t.getTableName();
                        List<FieldSchema> cols = t.getCols();
                        if (cols != null && !cols.isEmpty()) {
                            colNames = Utilities.getColumnNamesFromFieldSchema(cols);
                        }
                        break;
                    }
                }
            }

            Map<ColumnInfo, org.apache.hadoop.hive.ql.hooks.LineageInfo.Dependency> colMap = index.getDependencies(finalSelOp);
            List<org.apache.hadoop.hive.ql.hooks.LineageInfo.Dependency> dependencies = colMap != null ? Lists.newArrayList(colMap.values()) : null;
            int fields = fieldSchemas.size();
            if (t != null && colMap != null && fields < colMap.size()) {
                // Dynamic partition keys should be added to field schemas.
                List<FieldSchema> partitionKeys = t.getPartitionKeys();
                int dynamicKeyCount = colMap.size() - fields;
                int keyOffset = partitionKeys.size() - dynamicKeyCount;
                if (keyOffset >= 0) {
                    fields += dynamicKeyCount;
                    for (int i = 0; i < dynamicKeyCount; i++) {
                        FieldSchema field = partitionKeys.get(keyOffset + i);
                        fieldSchemas.add(field);
                        if (colNames != null) {
                            colNames.add(field.getName());
                        }
                    }
                }
            }
            if (dependencies == null || dependencies.size() != fields) {
                log("Result schema has " + fields
                        + " fields, but we don't get as many dependencies");
            } else {
                Set<Vertex> targets = new LinkedHashSet<Vertex>();
                for (int i = 0; i < fields; i++) {
                    Vertex target = getOrCreateVertex(vertexCache,
                            getTargetFieldName(i, destTableName, colNames, fieldSchemas),
                            Vertex.Type.COLUMN);
                    targets.add(target);
                    org.apache.hadoop.hive.ql.hooks.LineageInfo.Dependency dep = dependencies.get(i);
                    addEdge(vertexCache, edges, dep.getBaseCols(), target,
                            dep.getExpr(), Edge.Type.PROJECTION);
                }
                Set<org.apache.hadoop.hive.ql.hooks.LineageInfo.Predicate> conds = index.getPredicates(finalSelOp);
                if (conds != null && !conds.isEmpty()) {
                    for (org.apache.hadoop.hive.ql.hooks.LineageInfo.Predicate cond: conds) {
                        addEdge(vertexCache, edges, cond.getBaseCols(),
                                new LinkedHashSet<Vertex>(targets), cond.getExpr(),
                                Edge.Type.PREDICATE);
                    }
                }
            }

        }
        return edges;
    }

    /**
     * Convert a list of columns to a set of vertices.
     * Use cached vertices if possible.
     */
    private static Set<Vertex> createSourceVertices(
            Map<String, Vertex> vertexCache, Collection<BaseColumnInfo> baseCols) {
        Set<Vertex> sources = new LinkedHashSet<Vertex>();
        if (baseCols != null && !baseCols.isEmpty()) {
            for(BaseColumnInfo col: baseCols) {
                org.apache.hadoop.hive.metastore.api.Table table = col.getTabAlias().getTable();
                if (table.isTemporary()) {
                    // Ignore temporary tables
                    continue;
                }
                Vertex.Type type = Vertex.Type.TABLE;
                String tableName = Warehouse.getQualifiedName(table);
                FieldSchema fieldSchema = col.getColumn();
                String label = tableName;
                if (fieldSchema != null) {
                    type = Vertex.Type.COLUMN;
                    label = tableName + "." + fieldSchema.getName();
                }
                sources.add(getOrCreateVertex(vertexCache, label, type));
            }
        }
        return sources;
    }

    /**
     * Find an edge that has the same type, expression, and sources.
     */
    private static Edge findSimilarEdgeBySources(
            List<Edge> edges, Set<Vertex> sources, String expr, Edge.Type type) {
        for (Edge edge: edges) {
            if (edge.type == type && StringUtils.equals(edge.expr, expr)
                    && SetUtils.isEqualSet(edge.sources, sources)) {
                return edge;
            }
        }
        return null;
    }


    private static void addEdge(Map<String, Vertex> vertexCache, List<Edge> edges,
                                Set<org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo> srcCols, Set<Vertex> targets, String expr, Edge.Type type) {
        Set<Vertex> sources = createSourceVertices(vertexCache, srcCols);
        Edge edge = findSimilarEdgeBySources(edges, sources, expr, type);
        if (edge == null) {
            edges.add(new Edge(sources, targets, expr, type));
        } else {
            edge.targets.addAll(targets);
        }
    }

    private static void addEdge(Map<String, Vertex> vertexCache, List<Edge> edges,
                                Set<org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo> srcCols, Vertex target, String expr, Edge.Type type) {
        Set<Vertex> targets = new LinkedHashSet<Vertex>();
        targets.add(target);
        addEdge(vertexCache, edges, srcCols, targets, expr, type);
    }

    private static String getTargetFieldName(int fieldIndex,
                                             String destTableName, List<String> colNames, List<FieldSchema> fieldSchemas) {
        String fieldName = fieldSchemas.get(fieldIndex).getName();
        String[] parts = fieldName.split("\\.");
        if (destTableName != null) {
            String colName = parts[parts.length - 1];
            if (colNames != null && !colNames.contains(colName)) {
                colName = colNames.get(fieldIndex);
            }
            return destTableName + "." + colName;
        }
        if (parts.length == 2 && parts[0].startsWith("_u")) {
            return parts[1];
        }
        return fieldName;
    }

    private static Vertex getOrCreateVertex(
            Map<String, Vertex> vertices, String label, Vertex.Type type) {
        Vertex vertex = vertices.get(label);
        if (vertex == null) {
            vertex = new Vertex(label, type);
            vertices.put(label, vertex);
        }
        return vertex;
    }

    private static void log(String error) {
        SessionState.LogHelper console = SessionState.getConsole();
        if (console != null) {
            console.printError(error);
        }
    }
    private String getQueryHash(String queryStr) {
        Hasher hasher = Hashing.md5().newHasher();
        hasher.putBytes(queryStr.getBytes(Charset.defaultCharset()));
        return hasher.hash().toString();
    }

}
