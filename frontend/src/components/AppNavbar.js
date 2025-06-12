import * as React from 'react';
import { styled } from '@mui/material/styles';
import AppBar from '@mui/material/AppBar';
import Box from '@mui/material/Box';
import Stack from '@mui/material/Stack';
import MuiToolbar from '@mui/material/Toolbar';
import { tabsClasses } from '@mui/material/Tabs';
import Typography from '@mui/material/Typography';
import ColorModeIconDropdown from '../shared-theme/ColorModeIconDropdown';

const Toolbar = styled(MuiToolbar)({
    width: '100%',
    padding: '12px',
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'start',
    justifyContent: 'center',
    gap: '12px',
    flexShrink: 0,
    [`& ${tabsClasses.flexContainer}`]: {
        gap: '8px',
        p: '8px',
        pb: 0,
    },
});

export default function AppNavbar() {

    return (
        <AppBar
            position="fixed"
            sx={{
                // display: { xs: 'auto', md: 'none' },
                boxShadow: 0,
                bgcolor: 'background.paper',
                backgroundImage: 'none',
                borderBottom: '1px solid',
                borderColor: 'divider',
                top: 'var(--template-frame-height, 0px)',
            }}
        >
            <Toolbar variant="dense">
                <Stack
                    direction="row"
                    sx={{
                        alignItems: 'center',
                        flexGrow: 1,
                        width: '100%',
                        gap: 1,
                    }}
                >
                    <Stack
                        direction="row"
                        spacing={1}
                        sx={{ justifyContent: 'center', mr: 'auto' }}
                    >
                        <CustomIcon />
                        <Typography variant="h4" component="h1" sx={{ color: 'text.primary' }}>
                            HIV Sentinel Events Pathways
                        </Typography>
                    </Stack>

                    <ColorModeIconDropdown />
                </Stack>
            </Toolbar>
        </AppBar>
    );
}

export function CustomIcon() {
    return (
        <Box
            sx={{
                height: '1.5rem',
                borderRadius: '999px',
                display: 'flex',
                justifyContent: 'center',
                alignItems: 'center',
                alignSelf: 'center',
                ml: 3

            }}
        >
            <img src={'/assets/nascop-logo.png'} alt={'logo'} width={'180px'}/>
        </Box>
    );
}
