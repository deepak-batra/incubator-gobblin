/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the “License”); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.util;


import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import org.apache.avro.Schema;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.avro.TypeInfoToSchema;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * @author deepak.batra
 */
public class OrcUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OrcUtils.class);

    private static final String ORC_SUFFIX = ".orc";

    /**
     * Get the latest avro schema for a directory
     * @param directory the input dir that contains avro files
     * @param fs the {@link FileSystem} for the given directory.
     * @param latest true to return latest schema, false to return oldest schema
     * @return the latest/oldest schema as a pair of <column-names, column-type-infos> in the directory
     * @throws IOException
     */
    public static Optional<Pair<String, String>> getDirectorySchema(Path directory, FileSystem fs, boolean latest)
            throws
            IOException {
        Schema schema = null;
        Pair<String, String> pair = null;
        try (Closer closer = Closer.create()) {
            List<FileStatus> files = getDirectorySchemaHelper(directory, fs);
            if (files == null || files.size() == 0) {
                LOG.warn("There is no previous orc file in the directory:" + directory);
            } else {
                FileStatus file = latest ? files.get(0) : files.get(files.size() - 1);
                LOG.debug("Path to get the orc schema: " + file);
                FsInput fi = new FsInput(file.getPath(), fs.getConf());
                Reader reader = OrcFile.createReader(file.getPath(), OrcFile.readerOptions(fs.getConf()));
                List<? extends StructField> fields =
                        ((StructObjectInspector)reader.getObjectInspector()).getAllStructFieldRefs();
                TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(reader.getObjectInspector());
                List<String> columnNames = new ArrayList<>();
                List<TypeInfo> columnTypes = new ArrayList<>();
                StringBuilder columnsBuilder = new StringBuilder();
                StringBuilder columnTypesBuilder = new StringBuilder();
                for (int i=0; i < fields.size(); i++) {
                    columnNames.add(fields.get(i).getFieldName());
                    TypeInfo info = TypeInfoUtils.getTypeInfoFromObjectInspector(fields.get(i).getFieldObjectInspector());
                    columnTypes.add(info);
                    if (i > 0) {
                        columnsBuilder.append(",");
                        columnTypesBuilder.append(",");
                    }
                    columnsBuilder.append(fields.get(i).getFieldName());
                    columnTypesBuilder.append(info.getTypeName());
                }
                pair = new ImmutablePair<>(columnsBuilder.toString(), columnTypesBuilder.toString());
                schema = new TypeInfoToSchema().convert(columnNames, columnTypes,
                        Collections.emptyList(), null, null, null);
                closer.close();
            }
        } catch (IOException ioe) {
            throw new IOException("Cannot get the schema for directory " + directory, ioe);
        }
        return pair == null?Optional.absent():Optional.of(pair);
    }

    /**
     * Get the latest avro schema for a directory
     * @param directory the input dir that contains avro files
     * @param conf configuration
     * @param latest true to return latest schema, false to return oldest schema
     * @return the latest/oldest schema in the directory
     * @throws IOException
     */
    public static Optional<Pair<String, String>> getDirectorySchema(Path directory, Configuration conf, boolean latest)
            throws IOException {
        return getDirectorySchema(directory, FileSystem.get(conf), latest);
    }

    private static List<FileStatus> getDirectorySchemaHelper(Path directory, FileSystem fs) throws IOException {
        List<FileStatus> files = Lists.newArrayList();
        if (fs.exists(directory)) {
            getAllNestedOrcFiles(fs.getFileStatus(directory), files, fs);
            if (files.size() > 0) {
                Collections.sort(files, FileListUtils.LATEST_MOD_TIME_ORDER);
            }
        }
        return files;
    }

    private static void getAllNestedOrcFiles(FileStatus dir, List<FileStatus> files, FileSystem fs) throws IOException {
        if (dir.isDirectory()) {
            FileStatus[] filesInDir = fs.listStatus(dir.getPath());
            if (filesInDir != null) {
                for (FileStatus f : filesInDir) {
                    getAllNestedOrcFiles(f, files, fs);
                }
            }
        } else if (dir.getPath().getName().endsWith(ORC_SUFFIX)) {
            files.add(dir);
        }
    }

    /**
     * Parse Avro schema from a schema file.
     */
    public static Pair<String, String> parseSchemaFromFile(Path filePath, FileSystem fs) throws IOException {
        Preconditions.checkArgument(fs.exists(filePath), filePath + "does not exist");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
            String columns = reader.readLine();
            String types = reader.readLine();
            return new ImmutablePair<>(columns, types);
        }
    }


    public static void writeSchemaToFile(Pair<String, String> schema, Path filePath, FileSystem fs, boolean overwrite)
            throws IOException {
        writeSchemaToFile(schema, filePath, fs, overwrite, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ));
    }

    public static void writeSchemaToFile(Pair<String, String> schema, Path filePath, FileSystem fs, boolean overwrite, FsPermission perm)
            throws IOException {
        if (!overwrite) {
            Preconditions.checkState(!fs.exists(filePath), filePath + "already exists");
        } else {
            HadoopUtils.deletePath(fs, filePath, true);
        }

        try (DataOutputStream dos = fs.create(filePath)) {
            dos.writeChars(schema.getLeft());
            dos.writeChars("\n");
            dos.writeChars(schema.getRight());
        }
        fs.setPermission(filePath, perm);
    }
}
