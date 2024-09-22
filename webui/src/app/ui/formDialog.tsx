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

import {
    Alert,
    Button,
    Dialog,
    DialogActions,
    DialogContent,
    DialogTitle,
    Snackbar,
    TextField,
    Box,
    Chip,
    Typography,
    Autocomplete,
    MenuItem,
    Select,
    InputLabel,
    FormControl,
} from "@mui/material";
import React, { useCallback, useState, FormEvent } from "react";
  
  interface FormDialogProps {
    position: string;
    title: string;
    submitButtonLabel: string;
    formFields: {
      name: string;
      label: string;
      type: string;
      required?: boolean;
      values?: string[];
    }[];
    onSubmit: (formData: FormData) => Promise<string | undefined>;
  }
  
const FormDialog: React.FC<FormDialogProps> = ({
    position,
    title,
    submitButtonLabel,
    formFields,
    onSubmit,
}) => {
    const [showDialog, setShowDialog] = useState(false);
    const openDialog = useCallback(() => setShowDialog(true), []);
    const closeDialog = useCallback(() => setShowDialog(false), []);
    const [errorMessage, setErrorMessage] = useState("");
    const [arrayValues, setArrayValues] = useState<{ [key: string]: string[] }>({});
  
    const handleArrayChange = (name: string, value: string[]) => {
        setArrayValues({ ...arrayValues, [name]: value });
    };
  
    const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
        event.preventDefault();
        const formData = new FormData(event.currentTarget);
  
        Object.keys(arrayValues).forEach((name) => {
            formData.append(name, JSON.stringify(arrayValues[name]));
        });
  
        const error = await onSubmit(formData);
        if (error) {
            setErrorMessage(error);
        } else {
            closeDialog();
        }
    };
  
    return (
        <>
            {position === "card" ? (
                <Button variant="contained" onClick={openDialog}>
                    {title}
                </Button>
            ) : (
                <Button variant="outlined" onClick={openDialog}>
                    {title}
                </Button>
            )}
  
            <Dialog open={showDialog} onClose={closeDialog}>
                <form onSubmit={handleSubmit}>
                    <DialogTitle>{title}</DialogTitle>
                    <DialogContent sx={{ width: "500px" }}>
                        {formFields.map((field, index) =>
                            field.type === "array" ? (
                                <Box key={index} mb={2}>
                                    <Typography variant="subtitle1" className="mt-2 mb-2">
                                        {field.label}
                                    </Typography>
                                    <Autocomplete
                                        multiple
                                        freeSolo
                                        value={arrayValues[field.name] || []}
                                        options={[]}
                                        onChange={(event, newValue) =>
                                            handleArrayChange(field.name, newValue)
                                        }
                                        renderTags={(value, getTagProps) =>
                                            value.map((option, index) => (
                                                <Chip
                                                    {...getTagProps({ index })}
                                                    key={index}
                                                    label={option}
                                                />
                                            ))
                                        }
                                        renderInput={(params) => (
                                            <TextField
                                                {...params}
                                                variant="outlined"
                                                label={`Add ${field.label}*`}
                                                placeholder="Type and press enter"
                                            />
                                        )}
                                    />
                                </Box>
                            ) : field.type === "enum" ? (
                                <FormControl key={index} fullWidth sx={{ mt:3 }}>
                                    <InputLabel>{field.label}</InputLabel>
                                    <Select
                                        name={field.name}
                                        label={field.label}
                                        required={field.required}
                                        defaultValue=""
                                        multiple={false}
                                    >
                                        {field.values?.map((value, index) => (
                                            <MenuItem key={index} value={value}>
                                                {value}
                                            </MenuItem>
                                        ))}
                                    </Select>
                                </FormControl>
                            ) : (
                                <TextField
                                    key={index}
                                    autoFocus={index === 0}
                                    required={field.required}
                                    name={field.name}
                                    label={field.label}
                                    type={field.type}
                                    fullWidth
                                    variant="standard"
                                    margin="normal"
                                    sx={{ mb: 2 }}
                                />
                            )
                        )}
                    </DialogContent>
                    <DialogActions>
                        <Button onClick={closeDialog}>Cancel</Button>
                        <Button type="submit">{submitButtonLabel}</Button>
                    </DialogActions>
                </form>
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
        </>
    );
};
  
export default FormDialog;
  