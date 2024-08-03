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

import axios, { AxiosError } from "axios";

const apiPrefix = "/api/v1";
const apiHost = `${apiPrefix}`;

export interface Cluster {
  name: string;
  version: number;
  shards: {};
}

export async function fetchNamespaces(): Promise<string[]> {
    try {
        const { data: responseData } = await axios.get(`${apiHost}/namespaces`);
        return responseData.data.namespaces || [];
    } catch (error) {
        handleError(error);
        return [];
    }
}

export async function createNamespace(name: string): Promise<string> {
    try {
        const { data: responseData } = await axios.post(`${apiHost}/namespaces`, {
            namespace: name,
        });
        if (responseData?.data != undefined) {
            return "";
        } else {
            return handleError(responseData);
        }
    } catch (error) {
        return handleError(error);
    }
}

export async function deleteNamespace(name: string): Promise<string> {
    try {
        const { data: responseData } = await axios.delete(
            `${apiHost}/namespaces/${name}`
        );
        if (responseData?.data == "ok") {
            return "";
        } else {
            return handleError(responseData);
        }
    } catch (error) {
        return handleError(error);
    }
}

export async function createCluster(
    name: string,
    nodes: string[],
    replicas: number,
    password: string,
    namespace: string
): Promise<string> {
    try {
        const { data: responseData } = await axios.post(
            `${apiHost}/namespaces/${namespace}/clusters`,
            { name, nodes, replicas, password }
        );
        if (responseData?.data != undefined) {
            return "";
        } else {
            return handleError(responseData);
        }
    } catch (error) {
        return handleError(error);
    }
}

export async function fetchClusters(namespace: string): Promise<string[]> {
    try {
        const { data: responseData } = await axios.get(
            `${apiHost}/namespaces/${namespace}/clusters`
        );
        return responseData.data.clusters || [];
    } catch (error) {
        handleError(error);
        return [];
    }
}

export async function fetchCluster(
    namespace: string,
    cluster: string
): Promise<Object> {
    try {
        const { data: responseData } = await axios.get(
            `${apiHost}/namespaces/${namespace}/clusters/${cluster}`
        );
        return responseData.data.cluster;
    } catch (error) {
        handleError(error);
        return {};
    }
}

export async function deleteCluster(
    namespace: string,
    cluster: string
): Promise<string> {
    try {
        const { data: responseData } = await axios.delete(
            `${apiHost}/namespaces/${namespace}/clusters/${cluster}`
        );
        if (responseData?.data == "ok") {
            return "";
        } else {
            return handleError(responseData);
        }
    } catch (error) {
        return handleError(error);
    }
}

function handleError(error: any): string {
    let message: string = "";
    if (error instanceof AxiosError) {
        message = error.response?.data?.error?.message || error.message;
    } else if (error instanceof Error) {
        message = error.message;
    } else if (typeof error === "object") {
        message = error?.error?.message || error?.message;
    }
    return message || "Unknown error";
}
