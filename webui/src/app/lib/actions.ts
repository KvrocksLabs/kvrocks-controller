/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
"use server";

import { redirect } from "next/navigation";
import { createNamespace, deleteNamespace, createCluster, deleteCluster } from "./api";
import { revalidatePath } from "next/cache";

export async function createNamespaceAction(name: string): Promise<string> {
    const errMsg = await createNamespace(name);
    if (!errMsg) {
        revalidatePath("/cluster");
    }
    return errMsg;
}

export async function deleteNamespaceAction(name: string): Promise<string> {
    const result = await deleteNamespace(name);
    revalidatePath("/cluster");
    return result;
}

export async function createClusterAction(
    name: string,
    nodes: string[],
    replicas: number,
    password: string,
    namespace: string
): Promise<string> {
    const errMsg = await createCluster(
        name,
        nodes,
        replicas,
        password,
        namespace
    );
    if (!errMsg) {
        revalidatePath("/cluster");
    }
    return errMsg;
}

export async function deleteClusterAction(
    namespace: string,
    cluster: string
): Promise<string> {
    const result = await deleteCluster(cluster, namespace);
    revalidatePath("/cluster");
    return result;
}