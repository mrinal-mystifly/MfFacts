__author__ = 'mrinal'
import pandas as pd
import numpy as np
import sys
import datetime, sys
from dateutil.relativedelta import relativedelta
import os, re, json
pd.options.display.max_columns = 99

from Helpers.DatabaseHelper import create_database_connections
from Helpers.DatabaseHelper import create_azure_connections

class InsertMFFacts:
    def insertComponent(self):
        conns = create_database_connections()
        ats_conns = create_azure_connections()
        db_engine_pm = conns['pm_reportdb']
        db_engine_mfb = conns['mfb_myfarebox']
        if (len(sys.argv)) >= 3:
            try:
                start_date = datetime.date(int(sys.argv[1].split('-')[0]), int(sys.argv[1].split('-')[1]),int(sys.argv[1].split('-')[2]))
                end_date = datetime.date(int(sys.argv[1].split('-')[0]), int(sys.argv[1].split('-')[1]),int(sys.argv[1].split('-')[2]))
            except Exception as e:
                print("Something is wrong. Please put Start Date & End Date in YYYY-MM-DD format")
        else:
            start_date = datetime.date.today() - datetime.timedelta(days=1)
            end_date = datetime.date.today() - datetime.timedelta(days=1)



        pm_vad_query = '''
            SELECT Distinct
                MyFareBoxRef
                ,RIGHT(LEFT(MyFareBoxRef,(len(MyFareBoxRef)-2)), (LEN(MyFareBoxRef)-4)) as BookingRef
                ,Convert(varchar, [(C)IsGroup]) as IsGroup
                ,[AirlineCode]
                ,CPD.[ClientID]
                ,[ClientMCN]
                ,VAD.[ClientName]
                ,Convert(varchar, [ClientCountry]) as ClientCountry
                ,[ClientCurrency] as ClientCurrencyCode
                ,(SELECT Convert(date, Min(InvoiceDate))  From TransactionSalesHistory where MyFareBoxRef=VAD.MyFareBoxRef) as FirstInvoicedOn
                ,(select Count(*) from v_allData where MyFareBoxRef=VAD.MyFareBoxRef) as InvoiceCount
                ,[OriginCountryCode]
                ,[DestinationCountryCode]
                ,CASE
                    WHEN [OriginCountryCode] = [DestinationCountryCode] THEN 'Domestic' Else 'International'
                END AS 'IsInternational'
                ,(SELECT SUM([TotalSegments_Invoice])  From v_allData where MyFareBoxRef=VAD.MyFareBoxRef) as SegmentCount
                ,(SELECT SUM(TicketCount)  From v_allData where MyFareBoxRef=VAD.MyFareBoxRef) as TotalPaxCount
                ,[Segments]
                ,[Vendor] as TicketingVendor
                ,[VendorCurrency] as TicketingVendorCurrency
            FROM
                [ReportDb].[dbo].[V_AllData] as VAD
            LEFT JOIN
                [ReportDb].[dbo].[ClientProfileDetails] as CPD on VAD.ClientMCN = CPD.AccountNumber
            where DateOfInvoice >= '{}' and DateOfInvoice <= '{}' and len(MyFareBoxRef) >= 9
        '''.format(start_date, end_date)
        #print(pm_vad_query)
        df_pm_vad = pd.read_sql(pm_vad_query , db_engine_pm)
        booking_list = df_pm_vad['BookingRef'].unique().tolist()


        mfb_bd_query = '''
            SELECT
                Distinct
                [MyFareBox].[dbo].[fnRetrieveBookingRef](BD.BookingRef) as MyFareBoxRef,
                --BD.BookingRef as BookingRef,
                CONVERT(date, BD.CreatedOn)  as BookingDate,
                bd.bookingStatus as CurrentStatus,
                BSE.bookingStatus as CurrentStatusText
                ,(select flightId from [MyFareBox].[dbo].flightDetails where flightId=bI.FlightId and status = 4) as TicketedFlightID
                --,BD.ClientID
                --,CPD.AccountNumber as ClientMCN
                --,CPD.ClientName as ClientName
                --,CPD.MainCurrency as ClientCurrencyCode
                ,CPD.CountryCode as ClientCountryCode
                --,(select count(*) from [MyFareBox].[dbo].flightpassenger where bookingref=bd.bookingref) as TotalPaxCount
                ,(select count(*) from [MyFareBox].[dbo].flightpassenger where bookingref=bd.bookingref and passengerType = 0) as AdultPaxCount
                ,(select count(*) from [MyFareBox].[dbo].flightpassenger where bookingref=bd.bookingref and  passengerType = 1) as ChildPaxCount
                ,(select count(*) from [MyFareBox].[dbo].flightpassenger where bookingref=bd.bookingref and  passengerType = 2) as InfantPaxCount
                ,(select count(*) from [MyFareBox].[dbo].flightpassenger where bookingref=bd.bookingref and  passengerType = 3) as UknownPaxCount
                --,FD.isGroup as IsGroup
                --,FD.AirlineCode
                ,FD.PCC as BookingPCC
                --,(select Distinct Origin from [ReportDB].[dbo].transactionSalesHistory where MFRef=[MyFareBox].[dbo].[fnRetrieveBookingRef](BD.BookingRef)) as Origin
                --,(select Distinct Destination from [ReportDB].[dbo].transactionSalesHistory where MFRef=[MyFareBox].[dbo].[fnRetrieveBookingRef](BD.BookingRef)) as Destination
                --,(SELECT Top 1 CountryCode from [MyFareBox].[dbo].Airport where LangCode = 'EN' and AirportCode = (select Distinct Origin from [ReportDB].[dbo].transactionSalesHistory where MFRef=[MyFareBox].[dbo].[fnRetrieveBookingRef](BD.BookingRef)) ) as OriginCountryCode
                --,(SELECT Top 1 CountryCode from [MyFareBox].[dbo].Airport where LangCode = 'EN' and AirportCode = (select Distinct Destination from [ReportDB].[dbo].transactionSalesHistory where MFRef=[MyFareBox].[dbo].[fnRetrieveBookingRef](BD.BookingRef)) ) as DestinationCountryCode
                --,(select Distinct Segments from [ReportDB].[dbo].transactionSalesHistory where MFRef=[MyFareBox].[dbo].[fnRetrieveBookingRef](BD.BookingRef)) as Segments
                --,IsInternational
                ,(select  Distinct Top 1 FareMatrixId from [MyFareBox].[dbo].flightDetails where flightId=fd.flightId and Fd.Status = bd.BookingStatus) as FareMatrixApplied
                ,(select  Distinct Top 1 IsCommissionable from [MyFareBox].[dbo].flightDetails where flightId=fd.flightId and Fd.Status = bd.BookingStatus) as IsCommissionable
                ,(select  Distinct Top 1 CommSlabId from [MyFareBox].[dbo].flightDetails where flightId=fd.flightId and Fd.Status = bd.BookingStatus) as CommissionSlabApplied
                --,(select Count(*) from [ReportDB].[dbo].transactionSalesHistory where MFRef=[MyFareBox].[dbo].[fnRetrieveBookingRef](BD.BookingRef)) as InvoiceCount
                --,(select Convert(date, MIN(InvoiceDate)) from [ReportDB].[dbo].transactionSalesHistory where MFRef=[MyFareBox].[dbo].[fnRetrieveBookingRef](BD.BookingRef)) as FirstInvoicedOn
                --,(select  Distinct Top 1 VendorName from   [MyFareBox].[dbo].Vendor where vendorId=fd.vendorId) as TicketingVendor
                --,(select  Distinct Top 1 venCurrency from   [MyFareBox].[dbo].Vendor where vendorId=fd.vendorId) as TicketingVendorCurrency
                --,(select count(*) from [MyFareBox].[dbo].flightsegment where flightid=fd.flightid and SegmentStatus in (0,1)) as  Segmentcount
            FROM
                [MyFareBox].[dbo].[BookingDetails]  as BD
            LEFT JOIN
                [MyFareBox].[dbo].[BookItinerary]  as BI ON BD.BookingRef = BI.BookingRef
            LEFT JOIN
                [MyFareBox].[dbo].[FlightDetails]   as FD ON BI.FlightId = FD.FlightId
            LEFT JOIN
                [MyFareBox].[dbo].[BookingStatusEnum] as BSE ON bd.bookingStatus = BSE.[StatusEnum]
            LEFT JOIN
                [ReportDB].[dbo].[ClientProfileDetails] as CPD ON bd.ClientId = CPD.[ClientId]
            Where
                FD.Status = bd.bookingstatus and BD.BookingRef in {}
        '''.format(tuple(booking_list))
        #print(mfb_bd_query)


        df_mfb_bd = pd.read_sql(mfb_bd_query , db_engine_mfb)
        df_merge = pd.merge(df_pm_vad,df_mfb_bd, how='left',on='MyFareBoxRef')
        df_merge = df_merge[['MyFareBoxRef','BookingRef','BookingDate','CurrentStatus','CurrentStatusText','TicketedFlightID','ClientID','ClientMCN','ClientName','ClientCurrencyCode','ClientCountryCode','TotalPaxCount','AdultPaxCount','ChildPaxCount','InfantPaxCount','IsGroup','AirlineCode','BookingPCC','OriginCountryCode','DestinationCountryCode','Segments','IsInternational','FareMatrixApplied','IsCommissionable','CommissionSlabApplied','InvoiceCount','FirstInvoicedOn','TicketingVendor','TicketingVendorCurrency']]



        try:
            connection = db_engine_pm.raw_connection()
            cursor = connection.cursor()
        except Exception as e:
            print("Error in creating connection & getting Cursor: ")
        mf_exist = pd.read_sql('''SELECT DISTINCT MyFareBoxRef FROM mf_facts WHERE FirstInvoicedOn = '{}' '''.format(start_date),connection)
        mf_exist = mf_exist.MyFareBoxRef.tolist()
        df_merge['TicketedFlightID'].fillna(0, inplace=True)
        exception_list = []

        print("Insert & Update started ::: ")
        for index, row in df_merge.iterrows():
            if row[0] in mf_exist:
                try:
                    cursor.execute("UPDATE mf_facts SET MyFareBoxRef=?, BookingRef=?, BookingDate=?, CurrentStatus=?, CurrentStatusText=?, TicketedFlightID=?, ClientID=?, ClientMCN=?, ClientName=?, ClientCurrencyCode=?, ClientCountryCode=?, TotalPaxCount=?, AdultPaxCount=?, ChildPaxCount=?, InfantPaxCount=?, IsGroup=?, AirlineCode=?, BookingPCC=?, OriginCountryCode=?, DestinationCountryCode=?, Segments=?, IsInternational=?, FareMatrixApplied=?, IsCommissionable=?, CommissionSlabApplied=?, InvoiceCount=?, FirstInvoicedOn=?, TicketingVendor=?, TicketingVendorCurrency=?", (row[0], row[1], row[2],row[3], row[4], row[5], row[6], row[7], row[8],row[9], row[10], row[11],row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20],row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28]))
                    cursor.commit()
                    connection.close()
                except Exception as e:
                    continue

            try:
                cursor.execute("insert into mf_facts values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (row[0], row[1], row[2],row[3], row[4], row[5], row[6], row[7], row[8],row[9], row[10], row[11],row[12], row[13], int(row[14]), row[15], row[16], row[17], row[18], row[19], row[20],row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28]) )
                cursor.commit()
                connection.close()
                #print("Records Inserted for Date", row[0])
            except Exception as e:
                print(row[0], e)
                exception_list.append(row[0])
        cursor.commit()
        connection.close()

        print("Report Successfully updated to DB ")





