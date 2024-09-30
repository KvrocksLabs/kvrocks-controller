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

"use client";
import {
    Alert,
    Button,
    Dialog,
    DialogActions,
    DialogContent,
    DialogContentText,
    DialogTitle,
    IconButton,
    ListItem,
    ListItemButton,
    ListItemText,
    Menu,
    MenuItem,
    Snackbar,
    Tooltip,
} from "@mui/material";
import MoreHorizIcon from "@mui/icons-material/MoreHoriz";
import { useCallback, useRef, useState } from "react";
import { usePathname } from "next/navigation";
import { useRouter } from "next/navigation";
import {
    deleteCluster,
    deleteNamespace,
    deleteNode,
    deleteShard,
} from "../lib/api";
import { faTrash } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";

interface NamespaceItemProps {
  item: string;
  type: "namespace";
}

interface ClusterItemProps {
  item: string;
  type: "cluster";
  namespace: string;
}

interface ShardItemProps {
  item: string;
  type: "shard";
  namespace: string;
  cluster: string;
}

interface NodeItemProps {
  item: string;
  type: "node";
  namespace: string;
  cluster: string;
  shard: string;
  id: string;
}

type ItemProps =
  | NamespaceItemProps
  | ClusterItemProps
  | ShardItemProps
  | NodeItemProps;

export default function Item(props: ItemProps) {
    const { item, type } = props;
    const [hover, setHover] = useState<boolean>(false);
    const [showMenu, setShowMenu] = useState<boolean>(false);
    const listItemTextRef = useRef(null);
    const openMenu = useCallback(() => setShowMenu(true), []);
    const closeMenu = useCallback(
        () => (setShowMenu(false), setHover(false)),
        []
    );
    const [showDeleteConfirm, setShowDeleteConfirm] = useState<boolean>(false);
    const openDeleteConfirmDialog = useCallback(
        () => (setShowDeleteConfirm(true), closeMenu()),
        [closeMenu]
    );
    const closeDeleteConfirmDialog = useCallback(
        () => setShowDeleteConfirm(false),
        []
    );
    const [errorMessage, setErrorMessage] = useState<string>("");

    const router = useRouter();
    let activeItem = usePathname().split("/").pop() || "";

    const confirmDelete = useCallback(async () => {
        let response = "";
        if (type === "namespace") {
            response = await deleteNamespace(item);
            if (response === "") {
                router.push("/namespaces");
            }
            setErrorMessage(response);
            router.refresh();
        } else if (type === "cluster") {
            const { namespace } = props as ClusterItemProps;
            response = await deleteCluster(namespace, item);
            if (response === "") {
                router.push(`/namespaces/${namespace}`);
            }
            setErrorMessage(response);
            router.refresh();
        } else if (type === "shard") {
            const { namespace, cluster } = props as ShardItemProps;
            response = await deleteShard(
                namespace,
                cluster,
                (parseInt(item.split("\t")[1]) - 1).toString()
            );
            if (response === "") {
                router.push(`/namespaces/${namespace}/clusters/${cluster}`);
            }
            setErrorMessage(response);
            router.refresh();
        } else if (type === "node") {
            const { namespace, cluster, shard, id } = props as NodeItemProps;
            response = await deleteNode(namespace, cluster, shard, id);
            if (response === "") {
                router.push(
                    `/namespaces/${namespace}/clusters/${cluster}/shards/${shard}`
                );
            }
            setErrorMessage(response);
            router.refresh();
        }
        closeMenu();
    }, [item, type, props, closeMenu, router]);

    if (type === "shard") {
        activeItem = "Shard\t" + (parseInt(activeItem) + 1);
    }else if (type === "node") {
        activeItem = "Node\t" + (parseInt(activeItem) + 1);
    }
    const isActive = item === activeItem;

    return (
        <ListItem
            disablePadding
            secondaryAction={
                hover && (
                    <IconButton onClick={openMenu} ref={listItemTextRef}>
                        <MoreHorizIcon />
                    </IconButton>
                )
            }
            onMouseEnter={() => setHover(true)}
            onMouseLeave={() => !showMenu && setHover(false)}
            sx={{
                backgroundColor: isActive ? "rgba(0, 0, 0, 0.1)" : "transparent",
                "&:hover": {
                    backgroundColor: "rgba(0, 0, 0, 0.05)",
                },
            }}
        >
            <ListItemButton sx={{ paddingRight: "10px" }}>
                <Tooltip title={item} arrow>
                    <ListItemText
                        classes={{ primary: "overflow-hidden text-ellipsis text-nowrap" }}
                        primary={`${item}`}
                    />
                </Tooltip>
            </ListItemButton>
            <Menu
                id={item}
                open={showMenu}
                onClose={closeMenu}
                anchorEl={listItemTextRef.current}
                anchorOrigin={{
                    vertical: "center",
                    horizontal: "center",
                }}
            >
                <MenuItem onClick={openDeleteConfirmDialog}>
                    <FontAwesomeIcon icon={faTrash} color="red" />
                </MenuItem>
            </Menu>
            <Dialog open={showDeleteConfirm}>
                <DialogTitle>Confirm</DialogTitle>
                <DialogContent>
                    {type === "node" ? (
                        <DialogContentText>
              Please confirm you want to delete {item}
                        </DialogContentText>
                    ) : type === "shard" ? (
                        <DialogContentText>
              Please confirm you want to delete {item}
                        </DialogContentText>
                    ) : (
                        <DialogContentText>
              Please confirm you want to delete {type} {item}
                        </DialogContentText>
                    )}
                </DialogContent>
                <DialogActions>
                    <Button onClick={closeDeleteConfirmDialog}>Cancel</Button>
                    <Button onClick={confirmDelete} color="error">
            Delete
                    </Button>
                </DialogActions>
            </Dialog>
            <Snackbar
                open={!!errorMessage}
                autoHideDuration={5000}
                onClose={() => setErrorMessage("")}
                anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
            >
                <Alert
                    onClose={() => setErrorMessage("")}
                    severity="error"
                    variant="filled"
                    sx={{ width: "100%" }}
                >
                    {errorMessage}
                </Alert>
            </Snackbar>
        </ListItem>
    );
}
