/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.odps.ogg.handler.datahub.operations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public enum OperationTypes {
	
	INSERT(InsertOperationHandler.class),
	UPDATE(UpdateOperationHandler.class),
	UPDATE_AUDITCOMP(UpdateOperationHandler.class),
	UPDATE_FIELDCOMP(UpdateOperationHandler.class),
	UPDATE_FIELDCOMP_PK(UpdateOperationHandler.class),
	UNIFIED_UPDATE_VAL(UpdateOperationHandler.class),
	UNIFIED_PK_UPDATE_VAL(UpdateOperationHandler.class),
	DELETE(DeleteOperationHandler.class);
	
	final private Logger logger = LoggerFactory.getLogger(OperationTypes.class);
	
	private OperationHandler operationHandler;
	private OperationTypes(Class<?> clazz)
	{
		try {
			operationHandler = (OperationHandler) clazz.newInstance();
		} catch (Exception e) {
			logger.error("Unable to instantiate operation handler. "+ clazz , e);
		}
	}
	
	public OperationHandler getOperationHandler()
	{
		return operationHandler;
	}
}
