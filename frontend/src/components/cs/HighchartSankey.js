 import React, {useCallback, useEffect, useState} from 'react';
import {
    Box,
    Typography,
    Toolbar
} from '@mui/material';
import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';
import HC_exporting from 'highcharts/modules/exporting'
import SankeyModule from 'highcharts/modules/sankey';
import Grid from "@mui/material/Grid2";
import CustomizedDataGrid from "../CustomizedDataGrid";
import Stack from "@mui/material/Stack";
import Button from "@mui/material/Button";
import AppBar from "@mui/material/AppBar";
import DateRangePicker from 'rsuite/DateRangePicker';
import 'rsuite/DateRangePicker/styles/index.css';
import {Skeleton} from "@mui/lab";
import Filters from "./Filters";

SankeyModule(Highcharts);
HC_exporting(Highcharts);

const HighchartSankey = ({dataset}) => {
    const [data, setData] = useState([]);
    const [counties, setCounties] = useState([]);
    const [subCounties, setSubCounties] = useState([]);
    const [partners, setPartners] = useState([]);
    const [agencies, setAgencies] = useState([]);
    const sexes = ['Male', 'Female'];
    const ageGroup = ['Missing', ' Under 1', '01 to 04', '05 to 09', '10 to 14', '15 to 19', '20 to 24', '25 to 29', '30 to 34', '35 to 39', '40 to 44', '45 to 49', '50 to 54', '55 to 59', '60 to 64', '65+',]
    const [selectedAgencies, setSelectedAgencies] = useState([]);
    const [selectedPartners, setSelectedPartners] = useState([]);
    const [selectedCounties, setSelectedCounties] = useState([]);
    const [selectedSubCounties, setSelectedSubCounties] = useState([]);
    const [selectedSex, setSelectedSex] = useState([]);
    const [selectedAgeGroup, setSelectedAgeGroup] = useState([]);
    const [selectedNodeData, setSelectedNodeData] = useState([]); // For storing breakdown data
    const [selectedNode, setSelectedNode] = useState(''); // For storing the selected node name
    const [loading, setLoading] = useState(false); // For storing the selected node name
    const [loadingChart, setLoadingChart] = useState(false); // For storing the selected node name
    const [isDrawerOpen, setIsDrawerOpen] = useState(false);
    const [dateRange, setDateRange] = useState([
        new Date('2023-01-01'), new Date('2023-12-31')
    ]);

    const fetchData = useCallback(async () => {
        try {
            setLoadingChart(true)
            const filters = {
                Dataset: dataset,
                CohortYearMonthStart: dateRange[0] ? new Date(dateRange[0]).toISOString() : null,
                CohortYearMonthEnd: dateRange[1] ? new Date(dateRange[1]).toISOString() : null,
            };

            if (selectedAgencies.length > 0) {
                filters.Agency = selectedAgencies;
            }
            if (selectedPartners.length > 0) {
                filters.Partner = selectedPartners;
            }
            if (selectedCounties.length > 0) {
                filters.County = selectedCounties;
            }
            if (selectedSubCounties.length > 0) {
                filters.SubCounty = selectedSubCounties;
            }
            if (selectedSex.length > 0) {
                filters.Sex = selectedSex
            }
            if (selectedAgeGroup.length > 0) {
                filters.AgeGroup = selectedAgeGroup
            }
            const raw = JSON.stringify(filters);
            const myHeaders = new Headers();
            myHeaders.append("Content-Type", "application/json");

            const requestOptions = {
                method: "POST",
                headers: myHeaders,
                body: raw,
                redirect: "follow",
            };

            const apiUrl = process.env.REACT_APP_BACKEND_URL;
            let response = await fetch(`${apiUrl}`, requestOptions);
            response = await response.json();

            setData(response);
            setCounties(response?.uniqueCounties)
            setSubCounties(response?.uniqueSubCounties)
            setPartners(response?.uniquePartners)
            setAgencies(response?.uniqueAgencies)
        } catch (error) {
            console.error('Error fetching data:', error);
        } finally {
            setLoadingChart(false)
        }
    }, [dateRange, setData, selectedAgencies, selectedPartners, selectedCounties, selectedSubCounties, selectedSex, selectedAgeGroup]);


    const handleNodeClick = useCallback( async () => {
        let breakdown = []; // Get the breakdown data for the node
        setLoading(true)
        try{
            const myHeaders = new Headers();
            myHeaders.append("Content-Type", "application/json");

            let filters = {
                "node": selectedNode,
                CohortYearMonthStart: dateRange[0] ? new Date(dateRange[0]).toISOString() : null,
                CohortYearMonthEnd: dateRange[1] ? new Date(dateRange[1]).toISOString() : null
            }

            if (selectedAgencies.length > 0) {
                filters.Agency = selectedAgencies;
            }
            if (selectedPartners.length > 0) {
                filters.Partner = selectedPartners;
            }
            if (selectedCounties.length > 0) {
                filters.County = selectedCounties;
            }
            if (selectedSubCounties.length > 0) {
                filters.SubCounty = selectedSubCounties;
            }
            if (selectedSex.length > 0) {
                filters.Sex = selectedSex
            }
            if (selectedAgeGroup.length > 0) {
                filters.AgeGroup = selectedAgeGroup
            }

            const raw = JSON.stringify(filters);

            const requestOptions = {
                method: "POST",
                headers: myHeaders,
                body: raw,
                redirect: "follow"
            };

            const breakDownUrl = process.env.REACT_APP_BREAKDOWN_URL;
            let response = await fetch(`${breakDownUrl}`, requestOptions)
            breakdown = await response.json()
            breakdown = breakdown.map((table) => ({
                ...table,
                rows: table.rows.map((row, index) => ({ ...row, id: row.id || index}))
            }))
            setSelectedNodeData(breakdown);
        } catch (err){
            console.error(err)
        }
        setLoading(false)
    }, [selectedNode, dateRange, selectedAgencies, selectedPartners, selectedCounties, selectedSubCounties, selectedSex, selectedAgeGroup]);

    useEffect(() => {
        fetchData();
        handleNodeClick()
    }, [dateRange, fetchData, handleNodeClick]);

    const handleDateRangeChange = (newRange) => {
        setDateRange(newRange)
    }

    const highchartsOptions = {
        chart: {
            type: 'sankey',
        },
        exporting: {
            enabled: true
        },
        title: {
            text: ''
        },
        subtitle: {
            text:
                'Data as of end of January 2025'
        },
        plotOptions: {
            series: {
                events: {
                    click: function(event) {
                        setSelectedNode(event.point.name); // Handle node click event
                    }
                }
            }
        },
        series: [
            {
                keys: ['from', 'to', 'weight'],
                data: data?.sankeyData || [],
                type: 'sankey',
                name: 'Case breakdown'
            }
        ],
    };

    return (
        <Box
            sx={{
                position: 'relative', // For drawer positioning
                width: '100%',
                minWidth: '500px',
                height: '100%',
            }}
        >
            {loadingChart ?
                <Box
                    sx={{
                        p: 1.5,
                        width: '100%',
                        display: 'flex',
                        justifyContent: 'center',
                    }}
                >
                    <Skeleton
                        variant={'rectangular'}
                        height={'350px'}
                        width={'50%'}
                    />
                </Box> :
                <>
                    <AppBar position="static" sx={{ backgroundColor: "inherit", mb: 2, mt:-2 }}>
                        <Toolbar>
                            <Typography variant="h6" sx={{ flexGrow: 1 }} color={'black'}>
                            </Typography>
                            <Button
                                variant="outlined"
                                color="inherit"
                                onClick={() => setIsDrawerOpen(!isDrawerOpen)}
                            >
                                {isDrawerOpen ? 'Hide Filters' : 'Show Filters'}
                            </Button>
                        </Toolbar>
                    </AppBar>
                    <HighchartsReact highcharts={Highcharts} options={highchartsOptions} />
                </>
            }


            {selectedNode && (
                loading || loadingChart ?
                    <Skeleton animation="wave" /> :
                    <Box mt={4} sx={{zIndex: -1, overflow: 'auto'}}>
                        <Grid container spacing={2} columns={12}>
                            {selectedNodeData.map((node, id) => (
                                <Stack key={id}>
                                    <Typography variant="h6">{node?.tableTitle}</Typography>
                                    <Grid>
                                        <CustomizedDataGrid
                                            columns={node.columns}
                                            rows={node.rows}
                                            loading={loading}
                                        />
                                    </Grid>
                                </Stack>
                            ))}
                        </Grid>
                    </Box>

            )}
            {isDrawerOpen && (
                <Box
                    sx={{
                        position: 'absolute',
                        top: 64, // Accounts for the height of the AppBar
                        right: 0,
                        height: 'calc(100% - 64px)', // Subtract the AppBar height
                        backgroundColor: 'white',
                        boxShadow: '0px 0px 10px rgba(0, 0, 0, 0.1)',
                        p: 2,
                        overflow:'auto',
                    }}
                >
                    <Typography variant="h6" sx={{ mb: 2 }}>
                        Filters
                    </Typography>

                    <DateRangePicker placement={'bottomEnd'} value={dateRange} onChange={handleDateRangeChange} label="Cohort:"/>
                    <Filters
                        agencies={agencies}
                        partners={partners}
                        counties={counties}
                        subCounties={subCounties}
                        sexes={sexes}
                        ageGroups={ageGroup}
                        setSelectedAgencies={setSelectedAgencies}
                        setSelectedPartners={setSelectedPartners}
                        setSelectedCounties={setSelectedCounties}
                        setSelectedSubCounties={setSelectedSubCounties}
                        setSelectedSex={setSelectedSex}
                        setSelectedAgeGroup={setSelectedAgeGroup}
                        selectedCounties={selectedCounties}
                        selectedPartners={selectedPartners}
                        selectedAgencies={selectedAgencies}
                        selectedSubcounties={selectedSubCounties}
                        selectedSex={selectedSex}
                        selectedAgeGroup={selectedAgeGroup}
                    />

                </Box>
            )}
        </Box>
    );
};

export default HighchartSankey;
