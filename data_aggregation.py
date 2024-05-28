import pandas as pd
import os
import re
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime,text
from sqlalchemy.orm import declarative_base,sessionmaker

Base = declarative_base() 

#DEFINING THE SQL TABLES 
class ONSMonthly(Base):
    __tablename__ = 'ons_monthly_data'
    id = Column(Integer, primary_key=True)
    din_instante = Column(DateTime, index=True)
    ceg = Column(String)
    val_geracao = Column(Float)
class ONSHistQuarterlyData(Base):
    __tablename__ = 'ons_hist_quarterly_data'
    id = Column(Integer, primary_key=True)
    din_instante = Column(DateTime, index=True)
    ceg = Column(String(50))
    val_geracao = Column(Float)

class ONSQuarterly(Base):
    __tablename__ = 'ons_quarterly_data'
    id = Column(Integer, primary_key=True)
    din_instante = Column(DateTime, index=True)
    ceg = Column(String)
    val_geracao = Column(Float)
class EnevaProduction(Base):
    __tablename__ = 'eneva_production'
    id = Column(Integer, primary_key=True)
    plant = Column(String)
    quarter = Column(String)
    availability_percent = Column(Float)
    dispatch_percent = Column(Float, nullable=True)
    net_generation_gwh = Column(Float, nullable=True)
    gross_generation_gwh = Column(Float, nullable=True)
    generation_regulated_percent = Column(Float, nullable=True)
    generation_free_percent = Column(Float, nullable=True)
    variable_net_revenue = Column(Float, nullable=True)
    variable_contractual = Column(Float, nullable=True)
    capacity_factor_percent = Column(Float, nullable=True)
    generation_bil_percent = Column(Float, nullable=True)
    production_m_cubic = Column(Float, nullable=True)
    remaining_reserves = Column(Float, nullable=True)
    adjusted_factor = Column(Float,nullable=True)
    
##############agreggating and loading the data into the database task_code


def load_ONS_hist_quarterly_data(database_url='sqlite:///task_code.db', folder_ONS='ONS_data/monthly'):
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    ons_data = pd.DataFrame()
    ceg_dict = {
        "UTE.GN.MA.030800-5.01": "Maranhão III",
        "UTE.GN.MA.030202-3.01": "Maranhão IV",
        "UTE.GN.MA.030203-1.01": "Maranhão V",
        "UTE.GN.MA.030196-5.01": "Nova Venécia 2",
        "UTE.GN.MA.031193-6.01": "Parnaíba IV",
        "UTE.GN.MA.040562-0.01": "Parnaíba V",
        "UTE.GN.RR.044619-0.01": "Jaguatirica II",
        "UTE.GN.CE.028357-6.01": "Fortaleza",
        "UTE.GN.SE.032228-8.01": "Porto de Sergipe I",
        "UTE.CM.MA.029700-3.01": "Porto do Itaqui",
        "UTE.CM.CE.030098-5.01": "Porto do Pecém II"
    }

    for file in os.listdir(folder_ONS):
        if file.startswith('ONS_'):
            filepath = os.path.join(folder_ONS, file)
            print(f"Processing file: {filepath}")
            monthly_data = pd.read_csv(filepath)
            monthly_data['din_instante'] = pd.to_datetime(monthly_data['din_instante'])
            ons_data = pd.concat([ons_data, monthly_data], ignore_index=True)

    if ons_data.empty:
        print("No data loaded.")
        return

    filtered_ons_data = ons_data[ons_data['ceg'].isin(ceg_dict.keys())]

    aggregation_rules = {
        'val_geracao': 'sum'
    }

    ons_quarterly_data = filtered_ons_data.groupby([pd.Grouper(key='din_instante', freq='Q'), 'ceg']).agg(aggregation_rules).reset_index()
    ons_quarterly_data['din_instante'] = ons_quarterly_data['din_instante'].dt.to_period('Q').dt.to_timestamp()

    try:
        for _, row in ons_quarterly_data.iterrows():
            record = {
                'din_instante': row['din_instante'],
                'ceg': row['ceg'],
                'val_geracao': row['val_geracao'],
            }
            session.add(ONSHistQuarterlyData(**record))

        views_sql = [
        """
        CREATE VIEW IF NOT EXISTS Parnaiba_Consolidated_hist AS
        SELECT STRFTIME('%Y', din_instante) AS year, (CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3 AS quarter, SUM(val_geracao)/ 1000 AS total_val_geracao_gwh
        FROM ons_hist_quarterly_data
        WHERE ceg IN('UTE.GN.MA.030800-5.01', 'UTE.GN.MA.030202-3.01', 'UTE.GN.MA.030203-1.01', 'UTE.GN.MA.030196-5.01', 'UTE.GN.MA.031193-6.01', 'UTE.GN.MA.040562-0.01')
        GROUP BY STRFTIME('%Y', din_instante),(CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3;
        """
        ,
        """
        CREATE VIEW IF NOT EXISTS Coal_hist AS
        SELECT STRFTIME('%Y', din_instante) AS year, (CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3 AS quarter, SUM(val_geracao)/ 1000 AS total_val_geracao_gwh
        FROM ons_hist_quarterly_data
        WHERE ceg IN ('UTE.CM.MA.029700-3.01', 'UTE.CM.CE.030098-5.01')
        GROUP BY STRFTIME('%Y', din_instante),(CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3;
        """,
        """
        CREATE VIEW IF NOT EXISTS Roraima_hist AS
        SELECT STRFTIME('%Y', din_instante) AS year, (CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3 AS quarter, SUM(val_geracao)/ 1000 AS total_val_geracao_gwh
        FROM ons_hist_quarterly_data
        WHERE ceg = 'UTE.GN.RR.044619-0.01'
        GROUP BY STRFTIME('%Y', din_instante),(CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3;
        """,
        """
        CREATE VIEW IF NOT EXISTS Third_part_LNG_hist AS
        SELECT STRFTIME('%Y', din_instante) AS year, (CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3 AS quarter, SUM(val_geracao)/ 1000 AS total_val_geracao_gwh
        FROM ons_hist_quarterly_data
        WHERE ceg IN ('UTE.GN.CE.028357-6.01', 'UTE.GN.SE.032228-8.01')
        GROUP BY STRFTIME('%Y', din_instante),(CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3;
        """,
        """
        CREATE VIEW IF NOT EXISTS Total_Dispatch_hist AS
        SELECT STRFTIME('%Y', din_instante) AS year, (CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3 AS quarter, SUM(val_geracao)/ 1000 AS total_val_geracao_gwh
        FROM ons_hist_quarterly_data
        GROUP BY STRFTIME('%Y', din_instante),(CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3;
        """]
        with engine.connect() as connection:
            for view_sql in views_sql:
                connection.execute(text(view_sql))
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

    return ons_quarterly_data


    
def load_ONS_data(database_url='sqlite:///task_code.db', folder_ONS='ONS_data'):
    engine = create_engine(database_url)

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    ons_data = pd.DataFrame()
    ceg_dict = {
        "UTE.GN.MA.030800-5.01": "Maranhão III",
        "UTE.GN.MA.030202-3.01": "Maranhão IV",
        "UTE.GN.MA.030203-1.01": "Maranhão V",
        "UTE.GN.MA.030196-5.01": "Nova Venécia 2",
        "UTE.GN.MA.031193-6.01": "Parnaíba IV",
        "UTE.GN.MA.040562-0.01": "Parnaíba V",
        "UTE.GN.RR.044619-0.01": "Jaguatirica II",
        "UTE.GN.CE.028357-6.01": "Fortaleza",
        "UTE.GN.SE.032228-8.01": "Porto de Sergipe I",
        "UTE.CM.MA.029700-3.01": "Porto do Itaqui",
        "UTE.CM.CE.030098-5.01": "Porto do Pecém II"
    }

    for file in os.listdir(folder_ONS):
        if file.startswith('ONS_'):
            filepath = os.path.join(folder_ONS, file)
            print(f"Processing file: {filepath}")
            monthly_data = pd.read_csv(filepath)
            monthly_data['din_instante'] = pd.to_datetime(monthly_data['din_instante'])
            ons_data = pd.concat([ons_data, monthly_data], ignore_index=True)

    if ons_data.empty:
        print("No data loaded.")
        return

    filtered_ons_data = ons_data[ons_data['ceg'].isin(ceg_dict.keys())]

    aggregation_rules = {
        'val_geracao': 'sum'
    }

    ons_monthly_data = filtered_ons_data.groupby([pd.Grouper(key='din_instante', freq='M'), 'ceg']).agg(aggregation_rules).reset_index()
    ons_monthly_data['din_instante'] = ons_monthly_data['din_instante'].dt.to_period('M').dt.to_timestamp()

    ons_quarterly_data = filtered_ons_data.groupby([pd.Grouper(key='din_instante', freq='Q'), 'ceg']).agg(aggregation_rules).reset_index()
    ons_quarterly_data['din_instante'] = ons_quarterly_data['din_instante'].dt.to_period('Q').dt.to_timestamp()

    try:
        for index, row in ons_monthly_data.iterrows():
            record = {
                'din_instante': row['din_instante'],
                'ceg': row['ceg'],
                'val_geracao': row['val_geracao'],
            }
            session.add(ONSMonthly(**record))

        for index, row in ons_quarterly_data.iterrows():
            record = {
                'din_instante': row['din_instante'],
                'ceg': row['ceg'],
                'val_geracao': row['val_geracao'],
            }
            session.add(ONSQuarterly(**record))
        views_sql = [
        """
        CREATE VIEW IF NOT EXISTS Parnaiba_Consolidated_Quarterly AS
        SELECT STRFTIME('%Y', din_instante) AS year, (CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3 AS quarter, SUM(val_geracao)/ 1000 AS total_val_geracao_gwh
        FROM ons_quarterly_data
        WHERE ceg IN ('UTE.GN.MA.030800-5.01', 'UTE.GN.MA.030202-3.01', 'UTE.GN.MA.030203-1.01', 'UTE.GN.MA.030196-5.01', 'UTE.GN.MA.031193-6.01', 'UTE.GN.MA.040562-0.01')
        GROUP BY STRFTIME('%Y', din_instante),(CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3;
        """,
        """
        CREATE VIEW IF NOT EXISTS Parnaiba_Consolidated_Monthly AS
        SELECT DATE(din_instante) AS din_instante,SUM(val_geracao) / 1000 AS total_dispatch_gwh
        FROM ons_monthly_data
        WHERE ceg IN ('UTE.GN.MA.030800-5.01', 'UTE.GN.MA.030202-3.01', 'UTE.GN.MA.030203-1.01', 'UTE.GN.MA.030196-5.01', 'UTE.GN.MA.031193-6.01', 'UTE.GN.MA.040562-0.01')
        GROUP BY DATE(din_instante);
        """,
        """
        CREATE VIEW IF NOT EXISTS Coal_Consolidated_Quarterly AS
        SELECT STRFTIME('%Y', din_instante) AS year, (CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3 AS quarter, SUM(val_geracao)/ 1000 AS total_val_geracao_gwh
        FROM ons_quarterly_data
        WHERE ceg IN ('UTE.CM.MA.029700-3.01', 'UTE.CM.CE.030098-5.01')
        GROUP BY STRFTIME('%Y', din_instante),(CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3;
        """,
        """
        CREATE VIEW IF NOT EXISTS Coal_Consolidated_Monthly AS
       SELECT DATE(din_instante) AS din_instante,SUM(val_geracao) / 1000 AS total_dispatch_gwh
        FROM ons_monthly_data
        WHERE ceg IN ('UTE.CM.MA.029700-3.01', 'UTE.CM.CE.030098-5.01')
        GROUP BY DATE(din_instante);
        """,
        """
        CREATE VIEW IF NOT EXISTS Roraima_Quarterly AS
        SELECT STRFTIME('%Y', din_instante) AS year, (CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3 AS quarter, SUM(val_geracao)/ 1000 AS total_val_geracao_gwh
        FROM ons_quarterly_data
        WHERE ceg = 'UTE.GN.RR.044619-0.01'
        GROUP BY STRFTIME('%Y', din_instante),(CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3;
        """,
        """
        CREATE VIEW IF NOT EXISTS Roraima_Monthly AS
       SELECT DATE(din_instante) AS din_instante,SUM(val_geracao) / 1000 AS total_dispatch_gwh
        FROM ons_monthly_data
        WHERE ceg = 'UTE.GN.RR.044619-0.01'
        GROUP BY DATE(din_instante);
        """,
        """
        CREATE VIEW IF NOT EXISTS Third_part_LNG_Quarterly AS
        SELECT STRFTIME('%Y', din_instante) AS year, (CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3 AS quarter, SUM(val_geracao)/ 1000 AS total_val_geracao_gwh
        FROM ons_quarterly_data
        WHERE ceg IN ('UTE.GN.CE.028357-6.01', 'UTE.GN.SE.032228-8.01')
        GROUP BY STRFTIME('%Y', din_instante),(CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3;
        """,
        """
        CREATE VIEW IF NOT EXISTS Third_part_LNG_Monthly AS
        SELECT DATE(din_instante) AS din_instante,SUM(val_geracao) / 1000 AS total_dispatch_gwh
        FROM ons_monthly_data
        WHERE ceg IN ('UTE.GN.CE.028357-6.01', 'UTE.GN.SE.032228-8.01')
        GROUP BY DATE(din_instante);
        """,
         """
        CREATE VIEW IF NOT EXISTS Total_Dispatch_ONS_Quarterly AS
        SELECT STRFTIME('%Y', din_instante) AS year, (CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3 AS quarter, SUM(val_geracao)/ 1000 AS total_val_geracao_gwh
        FROM ons_quarterly_data
        GROUP BY STRFTIME('%Y', din_instante),(CAST(STRFTIME('%m', din_instante) AS INTEGER) + 2) / 3;
        """,
        """
        CREATE VIEW IF NOT EXISTS Total_Dispatch_ONS_Monthly AS
        SELECT DATE(din_instante) AS din_instante,SUM(val_geracao) / 1000 AS total_dispatch_gwh
        FROM ons_monthly_data
        GROUP BY DATE(din_instante);
        """]
        with engine.connect() as connection:
            for view_sql in views_sql:
                connection.execute(text(view_sql))
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

    return ons_monthly_data, ons_quarterly_data


def load_Eneva_data(database_url='sqlite:///task_code.db', filename='Eneva_data.csv', folder='Eneva_data'):
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    filepath = os.path.join(folder, filename)
    if not os.path.isfile(filepath):
        print(f"No data file found at {filepath}.")
        return pd.DataFrame()

    data_df = pd.read_csv(filepath)
    data_df.replace('N.A', np.nan, inplace=True)
    pivot_df = data_df.pivot_table(index=['Plant', 'Quarter'], columns='Variable', values='Value', aggfunc='first').reset_index()
    column_mapping = {
        'Plant': 'plant',
        'Quarter': 'quarter',
        'Availability (%)': 'availability_percent',
        'Dispatch (%)': 'dispatch_percent',
        'Net generation (GWh)': 'net_generation_gwh',
        'Gross Generation (GWh)': 'gross_generation_gwh',
        'Generation for Regulated Market (%)': 'generation_regulated_percent',
        'Generation for Free Market (%)': 'generation_free_percent',
        'Variable Net Revenue (R$/MWh)': 'variable_net_revenue',
        'Variable contractual revenue (R$/MWh)': 'variable_contractual',
        'Capacity Factor (%)': 'capacity_factor_percent',
        'Generation for Bilateral Contract (%)': 'generation_bil_percent',
        'Production (Bi m³)': 'production_m_cubic',
        'Remaining reserves (Bi m³)': 'remaining_reserves',
        'Adjusted Capacity Factor (%)':'adjusted_factor'
    }

    pivot_df.rename(columns=column_mapping, inplace=True)
    pivot_df.fillna(0, inplace=True)
    pivot_df.replace('-', np.nan, inplace=True)

    session = Session()
    try:
        for index, row in pivot_df.iterrows():
            session.add(EnevaProduction(**row.to_dict()))
        session.commit()
        views_sql = [
        """
        CREATE VIEW IF NOT EXISTS Coal_Eneva_Quarterly AS
        SELECT quarter, SUM(gross_generation_gwh) AS total_gross_generation_gwh
        FROM eneva_production
        WHERE plant IN ('Itaqui', 'Pecém')
        GROUP BY quarter;
        """,
        """
        CREATE VIEW IF NOT EXISTS Parnaiba_Eneva AS
        SELECT quarter, SUM(gross_generation_gwh) AS total_gross_generation_gwh
        FROM eneva_production
        WHERE plant IN ('Parnaíba I', 'Parnaíba II', 'Parnaíba III', 'Parnaíba IV', 'Parnaíba V')
        GROUP BY quarter;
        """,
        """
        CREATE VIEW IF NOT EXISTS Roraima_Eneva AS
        SELECT quarter, SUM(gross_generation_gwh) AS total_gross_generation_gwh
        FROM eneva_production
        WHERE plant = 'Jaguatirica II'
        GROUP BY quarter;
        """,
        """
        CREATE VIEW IF NOT EXISTS Third_party_LNG_Eneva AS
        SELECT quarter, SUM(gross_generation_gwh) AS total_gross_generation_gwh
        FROM eneva_production
        WHERE plant IN ('Porto de Sergipe', 'Fortaleza')
        GROUP BY quarter;
        """,
        """
        CREATE VIEW IF NOT EXISTS Total_Dispatch_Eneva AS
        SELECT quarter, SUM(gross_generation_gwh) AS total_gross_generation_gwh
        FROM eneva_production
        GROUP BY quarter;
        """]
        with engine.connect() as connection:
            for view_sql in views_sql:
                connection.execute(text(view_sql))

    except Exception as e:
        print(f"An error occurred: {e}")
        session.rollback()
    finally:
        session.close()  
    
    return pivot_df
