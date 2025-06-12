import {Autocomplete} from "@mui/lab";
import {autocompleteClasses, TextField, useTheme} from "@mui/material";
import React from "react";
import Box from "@mui/material/Box";
import {createTheme, ThemeProvider} from "@mui/material/styles";

const customTheme = (outerTheme) =>
    createTheme({
        palette: {
            mode: outerTheme.palette.mode,
        },
        components: {
            MuiAutocomplete: {
                defaultProps: {
                    renderOption: (props, option, state, ownerState) => {
                        const {key, ...optionProps} = props;
                        return (
                            <Box
                                key={key}
                                sx={{
                                    borderRadius: '8px',
                                    margin: '5px',
                                    [`&.${autocompleteClasses.option}`]: {
                                        padding: '8px',
                                    },
                                }}
                                component="li"
                                {...optionProps}
                            >
                                {ownerState.getOptionLabel(option)}
                            </Box>
                        );
                    },
                },
            },
        },
    });
const Filters = ({
                     agencies, partners, counties, subCounties, sexes, ageGroups,
                     setSelectedAgencies, setSelectedPartners, setSelectedCounties, setSelectedSubCounties, setSelectedSex, setSelectedAgeGroup,
                     selectedCounties, selectedSubcounties, selectedAgencies, selectedPartners, selectedSex, selectedAgeGroup
}) => {
    const outerTheme = useTheme();
    return (
        <ThemeProvider theme={customTheme(outerTheme)}>
            <Box sx={{display: 'flex', flexDirection: 'column', gap: 2, pt:2,
                maxWidth: "420px"}}>
                <Autocomplete
                    multiple
                    id="counties"
                    size="small"
                    limitTags={2}
                    options={counties}
                    getOptionLabel={(option) => option || 'NULL'}
                    filterSelectedOptions
                    value={selectedCounties}
                    onChange={(event, newValue) => setSelectedCounties(newValue)}
                    renderInput={(params) => (
                        <TextField
                            {...params}
                            label="County"
                            placeholder="Select County"
                        />
                    )}
                />
                <Autocomplete
                    multiple
                    id="subcounties"
                    size="small"
                    limitTags={2}
                    options={subCounties}
                    getOptionLabel={(option) => option || 'NULL'}
                    filterSelectedOptions
                    value={selectedSubcounties}
                    onChange={(event, newValue) => setSelectedSubCounties(newValue)}
                    renderInput={(params) => (
                        <TextField
                            {...params}
                            label="Sub County"
                            placeholder="Select Sub County"
                        />
                    )}
                />
                <Autocomplete
                    multiple
                    id="partners"
                    size="small"
                    limitTags={2}
                    options={partners}
                    getOptionLabel={(option) => option || 'NULL'}
                    filterSelectedOptions
                    value={selectedPartners}
                    onChange={(event, newValue) => setSelectedPartners(newValue)}
                    renderInput={(params) => (
                        <TextField
                            {...params}
                            label="Partner"
                            placeholder="Select Partner"
                        />
                    )}
                />
                <Autocomplete
                    multiple
                    id="agencies"
                    size="small"
                    limitTags={3}
                    options={agencies}
                    getOptionLabel={(option) => option || 'NULL'}
                    filterSelectedOptions
                    value={selectedAgencies}
                    onChange={(event, newValue) => setSelectedAgencies(newValue)}
                    renderInput={(params) => (
                        <TextField
                            {...params}
                            label="Agency"
                            placeholder="Select Agency"
                        />
                    )}
                />
                <Autocomplete
                    multiple
                    id="sexes"
                    size="small"
                    limitTags={3}
                    options={sexes}
                    getOptionLabel={(option) => option || 'NULL'}
                    filterSelectedOptions
                    value={selectedSex}
                    onChange={(event, newValue) => setSelectedSex(newValue)}
                    renderInput={(params) => (
                        <TextField
                            {...params}
                            label="Sex"
                            placeholder="Select Sex"
                        />
                    )}
                />
                <Autocomplete
                    multiple
                    id="agegroup"
                    size="small"
                    limitTags={3}
                    options={ageGroups}
                    getOptionLabel={(option) => option || 'NULL'}
                    filterSelectedOptions
                    value={selectedAgeGroup}
                    onChange={(event, newValue) => setSelectedAgeGroup(newValue)}
                    renderInput={(params) => (
                        <TextField
                            {...params}
                            label="Age Group"
                            placeholder="Select Age Group"
                        />
                    )}
                />
            </Box>
        </ThemeProvider>
    )
}

export default Filters
