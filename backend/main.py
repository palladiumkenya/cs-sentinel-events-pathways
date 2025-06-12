from pathlib import Path
from typing import Union

from annotated_types.test_cases import Case
from sqlalchemy.sql import text
from fastapi import FastAPI, Depends, Request
from sqlalchemy import func
from starlette.middleware.cors import CORSMiddleware

from database import get_db
from sqlalchemy.orm import Session
from models import CaseBreakdown, SankeyFilter, SankeyBreakdown, HeiCaseBreakdown

import pandas as pd
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI()
# Base.metadata.create_all(bind=engine)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# app.mount("/{rest_of_path: path}/{rest_of_path2: path}", StaticFiles(directory="build", html=True), name="static")

@app.post("/sankey-data/")
def get_sankey_data(filters: SankeyFilter, db: Session = Depends(get_db)):
    if filters.Dataset == 'hei':
        # Query for HeiCaseBreakdown
        query = db.query(
            HeiCaseBreakdown.ord,
            HeiCaseBreakdown.source,
            HeiCaseBreakdown.target,
            func.sum(HeiCaseBreakdown.metric).label('total_metric')
        )

        if filters.County:
            query = query.filter(HeiCaseBreakdown.County.in_(filters.County))
        if filters.SubCounty:
            query = query.filter(HeiCaseBreakdown.SubCounty.in_(filters.SubCounty))
        if filters.Agency:
            query = query.filter(HeiCaseBreakdown.AgencyName.in_(filters.Agency))
        if filters.CohortYearMonthStart:
            query = query.filter(HeiCaseBreakdown.CohortYearMonth >= filters.CohortYearMonthStart)
        else:
            query = query.filter(HeiCaseBreakdown.CohortYearMonth >= '2023-01-01')
        if filters.CohortYearMonthEnd:
            query = query.filter(HeiCaseBreakdown.CohortYearMonth <= filters.CohortYearMonthEnd)
        else:
            query = query.filter(HeiCaseBreakdown.CohortYearMonth < '2024-01-01')
        
        query = query.group_by(HeiCaseBreakdown.ord, HeiCaseBreakdown.source, HeiCaseBreakdown.target).order_by(HeiCaseBreakdown.ord)   
        data = query.all()

        # Transforming the data for Highcharts Sankey
        sankey_data = [
            {"from": record.source, "to": record.target, "weight": record.total_metric}
            for record in data
        ]

        unique_counties_query = db.query(HeiCaseBreakdown.County).filter(HeiCaseBreakdown.County != None).distinct().order_by(HeiCaseBreakdown.County)
        unique_subcounties_query = db.query(HeiCaseBreakdown.SubCounty).filter(HeiCaseBreakdown.SubCounty != None).distinct().order_by(HeiCaseBreakdown.SubCounty)
        # unique_partners = db.query(HeiCaseBreakdown.PartnerName).filter(HeiCaseBreakdown.PartnerName != None).distinct().order_by(HeiCaseBreakdown.PartnerName).all()
        unique_agencies = db.query(HeiCaseBreakdown.AgencyName).filter(HeiCaseBreakdown.AgencyName != None).distinct().order_by(HeiCaseBreakdown.AgencyName).all()

        if filters.County:
            unique_subcounties_query = unique_subcounties_query.filter(HeiCaseBreakdown.County.in_(filters.County))
        if filters.SubCounty:
            unique_counties_query = unique_counties_query.filter(HeiCaseBreakdown.SubCounty.in_(filters.SubCounty))

        unique_counties = unique_counties_query.all()
        unique_subcounties = unique_subcounties_query.all()

        return {
            "sankeyData": sankey_data,
            "uniqueCounties": [county[0] for county in unique_counties],
            "uniqueSubCounties": [subcounty[0] for subcounty in unique_subcounties],
            "uniquePartners": [],  # Hei does not have partners
            "uniqueAgencies": [agency[0] for agency in unique_agencies]
        }

    else:
        # Query for CaseBreakdown
        query = db.query(
        CaseBreakdown.ord,
        CaseBreakdown.source,
        CaseBreakdown.target,
        func.sum(CaseBreakdown.metric).label('total_metric')
        )

        if filters.County:
            query = query.filter(CaseBreakdown.County.in_(filters.County))
        if filters.SubCounty:
            query = query.filter(CaseBreakdown.SubCounty.in_(filters.SubCounty))
        if filters.Agency:
            query = query.filter(CaseBreakdown.AgencyName.in_(filters.Agency))
        if filters.Partner:
            query = query.filter(CaseBreakdown.PartnerName.in_(filters.Partner))
        if filters.Sex:
            query = query.filter(CaseBreakdown.Sex.in_(filters.Sex))
        if filters.AgeGroup:
            query = query.filter(CaseBreakdown.AgeGroup.in_(filters.AgeGroup))
        if filters.CohortYearMonthStart:
            query = query.filter(CaseBreakdown.CohortYearMonth >= filters.CohortYearMonthStart)
        else:
            query = query.filter(CaseBreakdown.CohortYearMonth >= '2023-01-01')
        if filters.CohortYearMonthEnd:
            query = query.filter(CaseBreakdown.CohortYearMonth <= filters.CohortYearMonthEnd)
        else:
            query = query.filter(CaseBreakdown.CohortYearMonth < '2024-01-01')

        query = query.group_by(CaseBreakdown.ord, CaseBreakdown.source, CaseBreakdown.target).order_by(CaseBreakdown.ord)   
        data = query.all()

        # Transforming the data for Highcharts Sankey
        sankey_data = [
            {"from": record.source, "to": record.target, "weight": record.total_metric}
            for record in data
        ]
        # Get unique values for counties, subcounties, partners, and agencies
        unique_counties_query = db.query(CaseBreakdown.County).filter(CaseBreakdown.County != None).distinct().order_by(CaseBreakdown.County)
        unique_subcounties_query = db.query(CaseBreakdown.SubCounty).filter(CaseBreakdown.SubCounty != None).distinct().order_by(CaseBreakdown.SubCounty)
        unique_partners = db.query(CaseBreakdown.PartnerName).filter(CaseBreakdown.PartnerName != None).distinct().order_by(CaseBreakdown.PartnerName).all()
        unique_agencies = db.query(CaseBreakdown.AgencyName).filter(CaseBreakdown.AgencyName != None).distinct().order_by(CaseBreakdown.AgencyName).all()

        if filters.County:
            unique_subcounties_query = unique_subcounties_query.filter(CaseBreakdown.County.in_(filters.County))
        if filters.SubCounty:
            unique_counties_query = unique_counties_query.filter(CaseBreakdown.SubCounty.in_(filters.SubCounty))

        unique_counties = unique_counties_query.all()
        unique_subcounties = unique_subcounties_query.all()

        return {
            "sankeyData": sankey_data,
            "uniqueCounties": [county[0] for county in unique_counties],
            "uniqueSubCounties": [subcounty[0] for subcounty in unique_subcounties],
            "uniquePartners": [partner[0] for partner in unique_partners],
            "uniqueAgencies": [agency[0] for agency in unique_agencies]
        }

@app.post("/sankey-data/breakdown")
def sankey_data_breakdown(node: SankeyBreakdown, db: Session = Depends(get_db)):
    result = []  # List to hold multiple tables
    filters = []
    filter_string = ""
    if node.Partner:
        filters.append(f"PartnerName IN {format_sql_in_clause(node.Partner)}")
    if node.Agency:
        filters.append(f"AgencyName IN {format_sql_in_clause(node.Agency)}")
    if node.County:
        filters.append(f"County IN {format_sql_in_clause(node.County)}")
    if node.SubCounty:
        filters.append(f"SubCounty IN {format_sql_in_clause(node.SubCounty)}")
    if node.Sex:
        filters.append(f"Sex IN {format_sql_in_clause(node.Sex)}")
    if node.AgeGroup:
        filters.append(f"AgeGroup IN {format_sql_in_clause(node.AgeGroup)}")

    # Combine filters with base query
    if filters:
        filter_string += " AND " + " AND ".join(filters)
    print(filter_string)

    if node.node in ['Total Cases Reported']:
        query2 = f"""
        SELECT 
            Sex,
            SUM(LinkedToART) AS Linked,
            SUM(NotLinkedOnART) AS NotLinked,
            SUM(WithoutBaselineCD4) AS InitialCD4Done,
            SUM(WithBaselineCD4) AS InitialCD4NotDone,
            SUM(AHD) AS WithAHD,
            SUM(CASE WHEN AHD = 0 THEN 1 ELSE 0 END) AS  WithoutAHD,
            SUM(NotStaged) AS NotStaged,
            SUM(WithInitialViralLoad) AS InitialViralLoadDone,
            SUM(WithoutInitialViralLoad) AS InitialViralLoadNotDone,
            SUM(IsSuppressedInitialViralload) AS InitialViralLoadSuppressed,
            SUM(IIF(CsSentinelEvents.IsSuppressedInitialViralload = 0, 1, 0)) AS InitialViralLoadUnsuppressed,
            SUM(RegimenChanged) AS RegimenChangeDone,
            SUM(RegimenNotChanged) AS RegimenChangeNotDone,
            SUM(LatestVLSuppressed) AS LatestVLSuppressed,
            SUM(LatestVLNotSuppressed) AS LatestVLUnsuppressed,
            SUM(PatientRetained) AS PatientsRetained,
            SUM(PatientNotRetained) AS PatientsNotRetained
        FROM CsSentinelEvents
        WHERE CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
        GROUP BY Sex
        """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "linked", "headerName": "Linked", "flex": 1, "minWidth": 100},
                {"field": "notLinked", "headerName": "Not Linked", "flex": 1, "minWidth": 100},
                {"field": "initialCD4Done", "headerName": "Initial CD4 Done", "flex": 1, "minWidth": 150},
                {"field": "initialCD4NotDone", "headerName": "Initial CD4 Not Done", "flex": 1, "minWidth": 150},
                {"field": "withAHD", "headerName": "With AHD", "flex": 1, "minWidth": 100},
                {"field": "WithoutAHD", "headerName": "Without AHD", "flex": 1, "minWidth": 100},
                {"field": "NotStaged", "headerName": "Not Staged", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadDone", "headerName": "Initial Viral Load Done", "flex": 1, "minWidth": 150},
                {"field": "InitialViralLoadNotDone", "headerName": "Initial Viral Load Not Done", "flex": 1, "minWidth": 150},
                {"field": "InitialViralLoadSuppressed", "headerName": "Initial Viral Load Suppressed", "flex": 1, "minWidth": 150},
                {"field": "InitialViralLoadUnsuppressed", "headerName": "Initial Viral Load Unsuppressed", "flex": 1, "minWidth": 150},
                {"field": "RegimenChangeDone", "headerName": "Regimen Change Done", "flex": 1, "minWidth": 150},
                {"field": "RegimenChangeNotDone", "headerName": "Regimen Change Not Done", "flex": 1, "minWidth": 150},
                {"field": "LatestVLSuppressed", "headerName": "Latest VL Suppressed", "flex": 1, "minWidth": 150},
                {"field": "LatestVLUnsuppressed", "headerName": "Latest VL Unsuppressed", "flex": 1, "minWidth": 150},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 150},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 150},
            ],
            "rows": [{
                "sex": row.Sex,
                "linked": row.Linked,
                "notLinked": row.NotLinked,
                "initialCD4Done": row.InitialCD4Done,
                "initialCD4NotDone": row.InitialCD4NotDone,
                "withAHD": row.WithAHD,
                "WithoutAHD": row.WithoutAHD,
                "NotStaged": row.NotStaged,
                "InitialViralLoadDone": row.InitialViralLoadDone,
                "InitialViralLoadNotDone": row.InitialViralLoadNotDone,
                "InitialViralLoadSuppressed": row.InitialViralLoadSuppressed,
                "InitialViralLoadUnsuppressed": row.InitialViralLoadUnsuppressed,
                "RegimenChangeDone": row.RegimenChangeDone,
                "RegimenChangeNotDone": row.RegimenChangeNotDone,
                "LatestVLSuppressed": row.LatestVLSuppressed,
                "LatestVLUnsuppressed": row.LatestVLUnsuppressed,
                "PatientsRetained": row.PatientsRetained,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Linked']:
        query2 = f"""
        SELECT 
            Sex,
            SUM(LinkedToART) AS Linked,
            SUM(WithoutBaselineCD4) AS InitialCD4Done,
            SUM(WithBaselineCD4) AS InitialCD4NotDone,
            SUM(AHD) AS WithAHD,
            SUM(CASE WHEN AHD = 0 THEN 1 ELSE 0 END) AS  WithoutAHD,
            SUM(NotStaged) AS NotStaged,
            SUM(WithInitialViralLoad) AS InitialViralLoadDone,
            SUM(WithoutInitialViralLoad) AS InitialViralLoadNotDone,
            SUM(IsSuppressedInitialViralload) AS InitialViralLoadSuppressed,
            SUM(IIF(CsSentinelEvents.IsSuppressedInitialViralload = 0, 1, 0)) AS InitialViralLoadUnsuppressed,
            SUM(RegimenChanged) AS RegimenChangeDone,
            SUM(RegimenNotChanged) AS RegimenChangeNotDone,
            SUM(LatestVLSuppressed) AS LatestVLSuppressed,
            SUM(LatestVLNotSuppressed) AS LatestVLUnsuppressed,
            SUM(PatientRetained) AS PatientsRetained,
            SUM(PatientNotRetained) AS PatientsNotRetained
        FROM CsSentinelEvents
        WHERE LinkedToART = 1 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
        GROUP BY Sex
        """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "linked", "headerName": "Linked", "flex": 1, "minWidth": 100},
                {"field": "initialCD4Done", "headerName": "Initial CD4 Done", "flex": 1, "minWidth": 100},
                {"field": "initialCD4NotDone", "headerName": "Initial CD4 Not Done", "flex": 1, "minWidth": 100},
                {"field": "withAHD", "headerName": "With AHD", "flex": 1, "minWidth": 100},
                {"field": "WithoutAHD", "headerName": "Without AHD", "flex": 1, "minWidth": 100},
                {"field": "NotStaged", "headerName": "Not Staged", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadDone", "headerName": "Initial Viral Load Done", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadNotDone", "headerName": "Initial Viral Load Not Done", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadSuppressed", "headerName": "Initial Viral Load Suppressed", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadUnsuppressed", "headerName": "Initial Viral Load Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeDone", "headerName": "Regimen Change Done", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeNotDone", "headerName": "Regimen Change Not Done", "flex": 1, "minWidth": 100},
                {"field": "LatestVLSuppressed", "headerName": "Latest VL Suppressed", "flex": 1, "minWidth": 100},
                {"field": "LatestVLUnsuppressed", "headerName": "Latest VL Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 100},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "linked": row.Linked,
                "initialCD4Done": row.InitialCD4Done,
                "initialCD4NotDone": row.InitialCD4NotDone,
                "withAHD": row.WithAHD,
                "WithoutAHD": row.WithoutAHD,
                "NotStaged": row.NotStaged,
                "InitialViralLoadDone": row.InitialViralLoadDone,
                "InitialViralLoadNotDone": row.InitialViralLoadNotDone,
                "InitialViralLoadSuppressed": row.InitialViralLoadSuppressed,
                "InitialViralLoadUnsuppressed": row.InitialViralLoadUnsuppressed,
                "RegimenChangeDone": row.RegimenChangeDone,
                "RegimenChangeNotDone": row.RegimenChangeNotDone,
                "LatestVLSuppressed": row.LatestVLSuppressed,
                "LatestVLUnsuppressed": row.LatestVLUnsuppressed,
                "PatientsRetained": row.PatientsRetained,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Initial CD4 Not Done']:

        query2 = f"""
        SELECT 
            Sex,
            SUM(WithoutBaselineCD4) AS InitialCD4NotDone,
            SUM(WithInitialViralLoad) AS InitialViralLoadDone,
            SUM(WithoutInitialViralLoad) AS InitialViralLoadNotDone,
            SUM(IsSuppressedInitialViralload) AS InitialViralLoadSuppressed,
            SUM(IIF(CsSentinelEvents.IsSuppressedInitialViralload = 0, 1, 0)) AS InitialViralLoadUnsuppressed,
            SUM(RegimenChanged) AS RegimenChangeDone,
            SUM(RegimenNotChanged) AS RegimenChangeNotDone,
            SUM(LatestVLSuppressed) AS LatestVLSuppressed,
            SUM(LatestVLNotSuppressed) AS LatestVLUnsuppressed,
            SUM(PatientRetained) AS PatientsRetained,
            SUM(PatientNotRetained) AS PatientsNotRetained
        FROM CsSentinelEvents
        WHERE LinkedToART = 1 and WithoutBaselineCD4 = 1 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
        GROUP BY Sex
        """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "initialCD4NotDone", "headerName": "Initial CD4 Not Done", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadDone", "headerName": "Initial Viral Load Done", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadNotDone", "headerName": "Initial Viral Load Not Done", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadSuppressed", "headerName": "Initial Viral Load Suppressed", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadUnsuppressed", "headerName": "Initial Viral Load Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeDone", "headerName": "Regimen Change Done", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeNotDone", "headerName": "Regimen Change Not Done", "flex": 1, "minWidth": 100},
                {"field": "LatestVLSuppressed", "headerName": "Latest VL Suppressed", "flex": 1, "minWidth": 100},
                {"field": "LatestVLUnsuppressed", "headerName": "Latest VL Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 100},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "initialCD4NotDone": row.InitialCD4NotDone,
                "InitialViralLoadDone": row.InitialViralLoadDone,
                "InitialViralLoadNotDone": row.InitialViralLoadNotDone,
                "InitialViralLoadSuppressed": row.InitialViralLoadSuppressed,
                "InitialViralLoadUnsuppressed": row.InitialViralLoadUnsuppressed,
                "RegimenChangeDone": row.RegimenChangeDone,
                "RegimenChangeNotDone": row.RegimenChangeNotDone,
                "LatestVLSuppressed": row.LatestVLSuppressed,
                "LatestVLUnsuppressed": row.LatestVLUnsuppressed,
                "PatientsRetained": row.PatientsRetained,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Initial CD4 Done']:
        query2 = f"""
        SELECT 
            Sex,
            SUM(WithBaselineCD4) AS InitialCD4Done,
            SUM(AHD) AS WithAHD,
            SUM(CASE WHEN AHD = 0 THEN 1 ELSE 0 END) AS  WithoutAHD,
            SUM(NotStaged) AS NotStaged,
            SUM(WithInitialViralLoad) AS InitialViralLoadDone,
            SUM(WithoutInitialViralLoad) AS InitialViralLoadNotDone,
            SUM(IsSuppressedInitialViralload) AS InitialViralLoadSuppressed,
            SUM(IIF(CsSentinelEvents.IsSuppressedInitialViralload = 0, 1, 0)) AS InitialViralLoadUnsuppressed,
            SUM(RegimenChanged) AS RegimenChangeDone,
            SUM(RegimenNotChanged) AS RegimenChangeNotDone,
            SUM(LatestVLSuppressed) AS LatestVLSuppressed,
            SUM(LatestVLNotSuppressed) AS LatestVLUnsuppressed,
            SUM(PatientRetained) AS PatientsRetained,
            SUM(PatientNotRetained) AS PatientsNotRetained
        FROM CsSentinelEvents
        WHERE LinkedToART = 1 and WithoutBaselineCD4 = 1 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
        GROUP BY Sex
        """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "initialCD4Done", "headerName": "Initial CD4 Done", "flex": 1, "minWidth": 100},
                {"field": "withAHD", "headerName": "With AHD", "flex": 1, "minWidth": 100},
                {"field": "WithoutAHD", "headerName": "Without AHD", "flex": 1, "minWidth": 100},
                {"field": "NotStaged", "headerName": "Not Staged", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadDone", "headerName": "Initial Viral Load Done", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadNotDone", "headerName": "Initial Viral Load Not Done", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadSuppressed", "headerName": "Initial Viral Load Suppressed", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadUnsuppressed", "headerName": "Initial Viral Load Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeDone", "headerName": "Regimen Change Done", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeNotDone", "headerName": "Regimen Change Not Done", "flex": 1, "minWidth": 100},
                {"field": "LatestVLSuppressed", "headerName": "Latest VL Suppressed", "flex": 1, "minWidth": 100},
                {"field": "LatestVLUnsuppressed", "headerName": "Latest VL Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 100},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "initialCD4Done": row.InitialCD4Done,
                "withAHD": row.WithAHD,
                "WithoutAHD": row.WithoutAHD,
                "NotStaged": row.NotStaged,
                "InitialViralLoadDone": row.InitialViralLoadDone,
                "InitialViralLoadNotDone": row.InitialViralLoadNotDone,
                "InitialViralLoadSuppressed": row.InitialViralLoadSuppressed,
                "InitialViralLoadUnsuppressed": row.InitialViralLoadUnsuppressed,
                "RegimenChangeDone": row.RegimenChangeDone,
                "RegimenChangeNotDone": row.RegimenChangeNotDone,
                "LatestVLSuppressed": row.LatestVLSuppressed,
                "LatestVLUnsuppressed": row.LatestVLUnsuppressed,
                "PatientsRetained": row.PatientsRetained,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Not Linked']:
        query1 = f"""
        SELECT Sex, SUM(NotLinkedToART) as number 
        FROM CsSentinelEvents 
        WHERE CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
        GROUP BY Sex;
        """
        data1 = db.execute(text(query1)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution Among New Cases Reported",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "number", "headerName": "Number", "flex": 1, "minWidth": 100}
            ],
            "rows": [{"sex": row.Sex, "number": row.number} for row in data1]
        })

    elif node.node in ['With AHD']:
        query2 = f"""
                SELECT 
                    Sex,
                    SUM(AHD) AS WithAHD,
                    SUM(WithInitialViralLoad) AS InitialViralLoadDone,
                    SUM(WithoutInitialViralLoad) AS InitialViralLoadNotDone,
                    SUM(IsSuppressedInitialViralload) AS InitialViralLoadSuppressed,
                    SUM(IIF(CsSentinelEvents.IsSuppressedInitialViralload = 0, 1, 0)) AS InitialViralLoadUnsuppressed,
                    SUM(RegimenChanged) AS RegimenChangeDone,
                    SUM(RegimenNotChanged) AS RegimenChangeNotDone,
                    SUM(LatestVLSuppressed) AS LatestVLSuppressed,
                    SUM(LatestVLNotSuppressed) AS LatestVLUnsuppressed,
                    SUM(PatientRetained) AS PatientsRetained,
                    SUM(PatientNotRetained) AS PatientsNotRetained
                FROM CsSentinelEvents
                WHERE LinkedToART = 1 and WithBaselineCD4 = 1 and AHD = 1 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
                GROUP BY Sex
                """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "withAHD", "headerName": "With AHD", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadDone", "headerName": "Initial Viral Load Done", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadNotDone", "headerName": "Initial Viral Load Not Done", "flex": 1,
                 "minWidth": 100},
                {"field": "InitialViralLoadSuppressed", "headerName": "Initial Viral Load Suppressed", "flex": 1,
                 "minWidth": 100},
                {"field": "InitialViralLoadUnsuppressed", "headerName": "Initial Viral Load Unsuppressed", "flex": 1,
                 "minWidth": 100},
                {"field": "RegimenChangeDone", "headerName": "Regimen Change Done", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeNotDone", "headerName": "Regimen Change Not Done", "flex": 1, "minWidth": 100},
                {"field": "LatestVLSuppressed", "headerName": "Latest VL Suppressed", "flex": 1, "minWidth": 100},
                {"field": "LatestVLUnsuppressed", "headerName": "Latest VL Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 100},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "withAHD": row.WithAHD,
                "InitialViralLoadDone": row.InitialViralLoadDone,
                "InitialViralLoadNotDone": row.InitialViralLoadNotDone,
                "InitialViralLoadSuppressed": row.InitialViralLoadSuppressed,
                "InitialViralLoadUnsuppressed": row.InitialViralLoadUnsuppressed,
                "RegimenChangeDone": row.RegimenChangeDone,
                "RegimenChangeNotDone": row.RegimenChangeNotDone,
                "LatestVLSuppressed": row.LatestVLSuppressed,
                "LatestVLUnsuppressed": row.LatestVLUnsuppressed,
                "PatientsRetained": row.PatientsRetained,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Without AHD']:
        query2 = f"""
                SELECT 
                    Sex,
                    SUM(CASE WHEN AHD = 0 THEN 1 ELSE 0 END) AS WithoutAHD,
                    SUM(WithInitialViralLoad) AS InitialViralLoadDone,
                    SUM(WithoutInitialViralLoad) AS InitialViralLoadNotDone,
                    SUM(IsSuppressedInitialViralload) AS InitialViralLoadSuppressed,
                    SUM(IIF(CsSentinelEvents.IsSuppressedInitialViralload = 0, 1, 0)) AS InitialViralLoadUnsuppressed,
                    SUM(RegimenChanged) AS RegimenChangeDone,
                    SUM(RegimenNotChanged) AS RegimenChangeNotDone,
                    SUM(LatestVLSuppressed) AS LatestVLSuppressed,
                    SUM(LatestVLNotSuppressed) AS LatestVLUnsuppressed,
                    SUM(PatientRetained) AS PatientsRetained,
                    SUM(PatientNotRetained) AS PatientsNotRetained
                FROM CsSentinelEvents
                WHERE LinkedToART = 1 and WithBaselineCD4 = 1 and AHD = 0 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
                GROUP BY Sex
                """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "WithoutAHD", "headerName": "Without AHD", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadDone", "headerName": "Initial Viral Load Done", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadNotDone", "headerName": "Initial Viral Load Not Done", "flex": 1,
                 "minWidth": 100},
                {"field": "InitialViralLoadSuppressed", "headerName": "Initial Viral Load Suppressed", "flex": 1,
                 "minWidth": 100},
                {"field": "InitialViralLoadUnsuppressed", "headerName": "Initial Viral Load Unsuppressed", "flex": 1,
                 "minWidth": 100},
                {"field": "RegimenChangeDone", "headerName": "Regimen Change Done", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeNotDone", "headerName": "Regimen Change Not Done", "flex": 1, "minWidth": 100},
                {"field": "LatestVLSuppressed", "headerName": "Latest VL Suppressed", "flex": 1, "minWidth": 100},
                {"field": "LatestVLUnsuppressed", "headerName": "Latest VL Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 100},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "WithoutAHD": row.WithoutAHD,
                "InitialViralLoadDone": row.InitialViralLoadDone,
                "InitialViralLoadNotDone": row.InitialViralLoadNotDone,
                "InitialViralLoadSuppressed": row.InitialViralLoadSuppressed,
                "InitialViralLoadUnsuppressed": row.InitialViralLoadUnsuppressed,
                "RegimenChangeDone": row.RegimenChangeDone,
                "RegimenChangeNotDone": row.RegimenChangeNotDone,
                "LatestVLSuppressed": row.LatestVLSuppressed,
                "LatestVLUnsuppressed": row.LatestVLUnsuppressed,
                "PatientsRetained": row.PatientsRetained,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Not Staged']:
        query2 = f"""
                SELECT 
                    Sex,
                    SUM(NotStaged) AS NotStaged,
                    SUM(WithInitialViralLoad) AS InitialViralLoadDone,
                    SUM(WithoutInitialViralLoad) AS InitialViralLoadNotDone,
                    SUM(IsSuppressedInitialViralload) AS InitialViralLoadSuppressed,
                    SUM(IIF(CsSentinelEvents.IsSuppressedInitialViralload = 0, 1, 0)) AS InitialViralLoadUnsuppressed,
                    SUM(RegimenChanged) AS RegimenChangeDone,
                    SUM(RegimenNotChanged) AS RegimenChangeNotDone,
                    SUM(LatestVLSuppressed) AS LatestVLSuppressed,
                    SUM(LatestVLNotSuppressed) AS LatestVLUnsuppressed,
                    SUM(PatientRetained) AS PatientsRetained,
                    SUM(PatientNotRetained) AS PatientsNotRetained
                FROM CsSentinelEvents
                WHERE LinkedToART = 1 and WithBaselineCD4 = 1 and NotStaged = 1 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
                GROUP BY Sex
                """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "NotStaged", "headerName": "Not Staged", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadDone", "headerName": "Initial Viral Load Done", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadNotDone", "headerName": "Initial Viral Load Not Done", "flex": 1,
                 "minWidth": 100},
                {"field": "InitialViralLoadSuppressed", "headerName": "Initial Viral Load Suppressed", "flex": 1,
                 "minWidth": 100},
                {"field": "InitialViralLoadUnsuppressed", "headerName": "Initial Viral Load Unsuppressed", "flex": 1,
                 "minWidth": 100},
                {"field": "RegimenChangeDone", "headerName": "Regimen Change Done", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeNotDone", "headerName": "Regimen Change Not Done", "flex": 1, "minWidth": 100},
                {"field": "LatestVLSuppressed", "headerName": "Latest VL Suppressed", "flex": 1, "minWidth": 100},
                {"field": "LatestVLUnsuppressed", "headerName": "Latest VL Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 100},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "NotStaged": row.NotStaged,
                "InitialViralLoadDone": row.InitialViralLoadDone,
                "InitialViralLoadNotDone": row.InitialViralLoadNotDone,
                "InitialViralLoadSuppressed": row.InitialViralLoadSuppressed,
                "InitialViralLoadUnsuppressed": row.InitialViralLoadUnsuppressed,
                "RegimenChangeDone": row.RegimenChangeDone,
                "RegimenChangeNotDone": row.RegimenChangeNotDone,
                "LatestVLSuppressed": row.LatestVLSuppressed,
                "LatestVLUnsuppressed": row.LatestVLUnsuppressed,
                "PatientsRetained": row.PatientsRetained,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Initial Viral Load Done']:
        query2 = f"""
                SELECT 
                    Sex,
                    SUM(WithInitialViralLoad) AS InitialViralLoadDone,
                    SUM(IsSuppressedInitialViralload) AS InitialViralLoadSuppressed,
                    SUM(IIF(CsSentinelEvents.IsSuppressedInitialViralload = 0, 1, 0)) AS InitialViralLoadUnsuppressed,
                    SUM(RegimenChanged) AS RegimenChangeDone,
                    SUM(RegimenNotChanged) AS RegimenChangeNotDone,
                    SUM(LatestVLSuppressed) AS LatestVLSuppressed,
                    SUM(LatestVLNotSuppressed) AS LatestVLUnsuppressed,
                    SUM(PatientRetained) AS PatientsRetained,
                    SUM(PatientNotRetained) AS PatientsNotRetained
                FROM CsSentinelEvents
                WHERE LinkedToART = 1 and WithInitialViralLoad = 1 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
                GROUP BY Sex
                """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadDone", "headerName": "Initial Viral Load Done", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadSuppressed", "headerName": "Initial Viral Load Suppressed", "flex": 1,
                 "minWidth": 100},
                {"field": "InitialViralLoadUnsuppressed", "headerName": "Initial Viral Load Unsuppressed", "flex": 1,
                 "minWidth": 100},
                {"field": "RegimenChangeDone", "headerName": "Regimen Change Done", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeNotDone", "headerName": "Regimen Change Not Done", "flex": 1, "minWidth": 100},
                {"field": "LatestVLSuppressed", "headerName": "Latest VL Suppressed", "flex": 1, "minWidth": 100},
                {"field": "LatestVLUnsuppressed", "headerName": "Latest VL Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 100},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "InitialViralLoadDone": row.InitialViralLoadDone,
                "InitialViralLoadSuppressed": row.InitialViralLoadSuppressed,
                "InitialViralLoadUnsuppressed": row.InitialViralLoadUnsuppressed,
                "RegimenChangeDone": row.RegimenChangeDone,
                "RegimenChangeNotDone": row.RegimenChangeNotDone,
                "LatestVLSuppressed": row.LatestVLSuppressed,
                "LatestVLUnsuppressed": row.LatestVLUnsuppressed,
                "PatientsRetained": row.PatientsRetained,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Initial Viral Load Not Done']:
        query2 = f"""
                SELECT 
                    Sex,
                    SUM(WithoutInitialViralLoad) AS InitialViralLoadNotDone,
                    SUM(RegimenChanged) AS RegimenChangeDone,
                    SUM(RegimenNotChanged) AS RegimenChangeNotDone,
                    SUM(LatestVLSuppressed) AS LatestVLSuppressed,
                    SUM(LatestVLNotSuppressed) AS LatestVLUnsuppressed,
                    SUM(PatientRetained) AS PatientsRetained,
                    SUM(PatientNotRetained) AS PatientsNotRetained
                FROM CsSentinelEvents
                WHERE LinkedToART = 1 and WithoutInitialViralLoad = 1 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
                GROUP BY Sex
                """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadNotDone", "headerName": "Initial Viral Load Not Done", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeDone", "headerName": "Regimen Change Done", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeNotDone", "headerName": "Regimen Change Not Done", "flex": 1, "minWidth": 100},
                {"field": "LatestVLSuppressed", "headerName": "Latest VL Suppressed", "flex": 1, "minWidth": 100},
                {"field": "LatestVLUnsuppressed", "headerName": "Latest VL Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 100},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "InitialViralLoadNotDone": row.InitialViralLoadNotDone,
                "RegimenChangeDone": row.RegimenChangeDone,
                "RegimenChangeNotDone": row.RegimenChangeNotDone,
                "LatestVLSuppressed": row.LatestVLSuppressed,
                "LatestVLUnsuppressed": row.LatestVLUnsuppressed,
                "PatientsRetained": row.PatientsRetained,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Initial Viral Load Suppressed']:
        query2 = f"""
                SELECT 
                    Sex,
                    SUM(IsSuppressedInitialViralload) AS InitialViralLoadSuppressed,
                    SUM(RegimenChanged) AS RegimenChangeDone,
                    SUM(RegimenNotChanged) AS RegimenChangeNotDone,
                    SUM(LatestVLSuppressed) AS LatestVLSuppressed,
                    SUM(LatestVLNotSuppressed) AS LatestVLUnsuppressed,
                    SUM(PatientRetained) AS PatientsRetained,
                    SUM(PatientNotRetained) AS PatientsNotRetained
                FROM CsSentinelEvents
                WHERE LinkedToART = 1 and IsSuppressedInitialViralload = 1 and WithoutInitialViralLoad = 0 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
                GROUP BY Sex
                """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadSuppressed", "headerName": "Initial Viral Load Suppressed", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeDone", "headerName": "Regimen Change Done", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeNotDone", "headerName": "Regimen Change Not Done", "flex": 1, "minWidth": 100},
                {"field": "LatestVLSuppressed", "headerName": "Latest VL Suppressed", "flex": 1, "minWidth": 100},
                {"field": "LatestVLUnsuppressed", "headerName": "Latest VL Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 100},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "InitialViralLoadSuppressed": row.InitialViralLoadSuppressed,
                "RegimenChangeDone": row.RegimenChangeDone,
                "RegimenChangeNotDone": row.RegimenChangeNotDone,
                "LatestVLSuppressed": row.LatestVLSuppressed,
                "LatestVLUnsuppressed": row.LatestVLUnsuppressed,
                "PatientsRetained": row.PatientsRetained,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Initial Viral Load Unsuppressed']:
        query2 = f"""
                SELECT 
                    Sex,
                    SUM(IIF(CsSentinelEvents.IsSuppressedInitialViralload = 0, 1, 0)) AS InitialViralLoadUnsuppressed,
                    SUM(RegimenChanged) AS RegimenChangeDone,
                    SUM(RegimenNotChanged) AS RegimenChangeNotDone,
                    SUM(LatestVLSuppressed) AS LatestVLSuppressed,
                    SUM(LatestVLNotSuppressed) AS LatestVLUnsuppressed,
                    SUM(PatientRetained) AS PatientsRetained,
                    SUM(PatientNotRetained) AS PatientsNotRetained
                FROM CsSentinelEvents
                WHERE LinkedToART = 1 and IsSuppressedInitialViralload = 0 and WithoutInitialViralLoad = 0 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
                GROUP BY Sex
                """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadUnsuppressed", "headerName": "Initial Viral Load Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeDone", "headerName": "Regimen Change Done", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeNotDone", "headerName": "Regimen Change Not Done", "flex": 1, "minWidth": 100},
                {"field": "LatestVLSuppressed", "headerName": "Latest VL Suppressed", "flex": 1, "minWidth": 100},
                {"field": "LatestVLUnsuppressed", "headerName": "Latest VL Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 100},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "InitialViralLoadUnsuppressed": row.InitialViralLoadUnsuppressed,
                "RegimenChangeDone": row.RegimenChangeDone,
                "RegimenChangeNotDone": row.RegimenChangeNotDone,
                "LatestVLSuppressed": row.LatestVLSuppressed,
                "LatestVLUnsuppressed": row.LatestVLUnsuppressed,
                "PatientsRetained": row.PatientsRetained,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Regimen Change Not Done']:
        query2 = f"""
                SELECT 
                    Sex,
                    SUM(RegimenNotChanged) AS RegimenChangeNotDone,
                    SUM(LatestVLSuppressed) AS LatestVLSuppressed,
                    SUM(LatestVLNotSuppressed) AS LatestVLUnsuppressed,
                    SUM(PatientRetained) AS PatientsRetained,
                    SUM(PatientNotRetained) AS PatientsNotRetained
                FROM CsSentinelEvents
                WHERE LinkedToART = 1 and RegimenChangeNotDone = 1 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
                GROUP BY Sex
                """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "InitialViralLoadUnsuppressed", "headerName": "Initial Viral Load Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeNotDone", "headerName": "Regimen Change Not Done", "flex": 1, "minWidth": 100},
                {"field": "LatestVLSuppressed", "headerName": "Latest VL Suppressed", "flex": 1, "minWidth": 100},
                {"field": "LatestVLUnsuppressed", "headerName": "Latest VL Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 100},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "InitialViralLoadUnsuppressed": row.InitialViralLoadUnsuppressed,
                "RegimenChangeNotDone": row.RegimenChangeNotDone,
                "LatestVLSuppressed": row.LatestVLSuppressed,
                "LatestVLUnsuppressed": row.LatestVLUnsuppressed,
                "PatientsRetained": row.PatientsRetained,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Regimen Change Done']:
        query2 = f"""
                SELECT 
                    Sex,
                    SUM(RegimenChanged) AS RegimenChangeDone,
                    SUM(LatestVLSuppressed) AS LatestVLSuppressed,
                    SUM(LatestVLNotSuppressed) AS LatestVLUnsuppressed,
                    SUM(PatientRetained) AS PatientsRetained,
                    SUM(PatientNotRetained) AS PatientsNotRetained
                FROM CsSentinelEvents
                WHERE LinkedToART = 1 and RegimenChanged = 1 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
                GROUP BY Sex
                """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "RegimenChangeDone", "headerName": "Regimen Change Done", "flex": 1, "minWidth": 100},
                {"field": "LatestVLSuppressed", "headerName": "Latest VL Suppressed", "flex": 1, "minWidth": 100},
                {"field": "LatestVLUnsuppressed", "headerName": "Latest VL Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 100},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "RegimenChangeDone": row.RegimenChangeDone,
                "LatestVLSuppressed": row.LatestVLSuppressed,
                "LatestVLUnsuppressed": row.LatestVLUnsuppressed,
                "PatientsRetained": row.PatientsRetained,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Latest Viral Load Unsuppressed']:
        query2 = f"""
                SELECT 
                    Sex,
                    SUM(LatestVLNotSuppressed) AS LatestVLUnsuppressed,
                    SUM(PatientRetained) AS PatientsRetained,
                    SUM(PatientNotRetained) AS PatientsNotRetained
                FROM CsSentinelEvents
                WHERE LinkedToART = 1 and LatestVLNotSuppressed = 1 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
                GROUP BY Sex
                """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "LatestVLUnsuppressed", "headerName": "Latest VL Unsuppressed", "flex": 1, "minWidth": 100},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 100},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "LatestVLUnsuppressed": row.LatestVLUnsuppressed,
                "PatientsRetained": row.PatientsRetained,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Latest Viral Load Suppressed']:
        query2 = f"""
                SELECT 
                    Sex,
                    SUM(LatestVLSuppressed) AS LatestVLSuppressed,
                    SUM(PatientRetained) AS PatientsRetained,
                    SUM(PatientNotRetained) AS PatientsNotRetained
                FROM CsSentinelEvents
                WHERE LinkedToART = 1 and LatestVLSuppressed = 1 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
                GROUP BY Sex
                """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "LatestVLSuppressed", "headerName": "Latest VL Suppressed", "flex": 1, "minWidth": 100},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 100},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "LatestVLSuppressed": row.LatestVLSuppressed,
                "PatientsRetained": row.PatientsRetained,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Patients Not Retained']:
        query2 = f"""
                SELECT 
                    Sex,
                    SUM(PatientNotRetained) AS PatientsNotRetained
                FROM CsSentinelEvents
                WHERE LinkedToART = 1 and PatientsNotRetained = 1 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
                GROUP BY Sex
                """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "PatientsNotRetained", "headerName": "Patients Not Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "PatientsNotRetained": row.PatientsNotRetained,
            } for row in data2]
        })

    elif node.node in ['Patients Retained']:
        query2 = f"""
                SELECT 
                    Sex,
                    SUM(PatientRetained) AS PatientsRetained
                FROM CsSentinelEvents
                WHERE LinkedToART = 1 and PatientsRetained = 1 and CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
                GROUP BY Sex
                """
        data2 = db.execute(text(query2)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution For Followup Events",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "PatientsRetained", "headerName": "Patients Retained", "flex": 1, "minWidth": 100},
            ],
            "rows": [{
                "sex": row.Sex,
                "PatientsRetained": row.PatientsRetained,
            } for row in data2]
        })


    elif 'highcharts' in node.node:
        return []
    else:
        # Table 1: Sex and LinkedToART
        query1 = f"""
            SELECT Sex, SUM(LinkedToART) as number 
            FROM CsSentinelEvents 
            WHERE CohortYearMonth >= '{node.CohortYearMonthStart}' and CohortYearMonth <= '{node.CohortYearMonthEnd}' {filter_string}
            GROUP BY Sex;
        """
        data1 = db.execute(text(query1)).fetchall()

        result.append({
            "tableTitle": f"Sex Distribution",
            "columns": [
                {"field": "sex", "headerName": "Sex", "flex": 1, "minWidth": 100},
                {"field": "number", "headerName": "Number", "flex": 1, "minWidth": 100}
            ],
            "rows": [{"sex": row.Sex, "number": row.number} for row in data1]
        })

    return result


def format_sql_in_clause(values):
    if isinstance(values, (list, tuple)) and len(values) == 1:
        return f"('{values[0]}')"
    return str(tuple(values))


@app.get("/health")
async def health_check():
    return {"message": "OK"}
