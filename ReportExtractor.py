import streamlit as st
import pandas as pd
import json
from zipfile import ZipFile
import shutil
import os

class ReportExtractor:
    def __init__(self, path, name):
        self.path = path
        self.name = name
        self.result = []

    def extract(self):
        pathFolder = f'{self.path}/{self.name[:-5]}'
        report_name = self.name[:-5]

        try:
            shutil.rmtree(pathFolder)
        except FileNotFoundError:
            st.write(f'Folder {pathFolder} not present')

        with ZipFile(f'{self.path}/{self.name}', 'r') as f:
            f.extractall(pathFolder)

        report_layout = json.loads(open(f'{pathFolder}/Report/Layout', 'r', encoding='utf-16 le').read())
        data_model_raw = json.loads(open(f'{pathFolder}/DataModelSchema', 'r', encoding='utf-16 le').read())

        fields = []
        data_model = []

        for i in data_model_raw['model']['tables']:
            Name = i['name']
            Mode = i['partitions'][0]['mode']
            Type = i['partitions'][0]['source']['type']
            try:
                Source = i['partitions'][0]['source']['expressionSource']
            except KeyError:
                if 'expression' in i['partitions'][0]['source']:
                	Source = i['partitions'][0]['source']['expression']
                else:Source: [""] # type: ignore
            data_model.append([report_name, Name, Mode, Type, Source])

        data_model_df = pd.DataFrame(columns=['Report Name', 'Name', 'Mode', 'Type', 'Source'], data=data_model)

        measures = []

        for i in data_model_raw['model']['tables']:
            Name = i['name']
            try:
                for j in i['measures']:
                    Measure_Name = j['name']
                    Measure_Expression = j['expression']
                    measures.append([report_name, Name, Measure_Name, Measure_Expression])
            except KeyError:
                pass

        measures_df = pd.DataFrame(columns=['Report Name', 'Name', 'Measure_Name', 'Measure_Expression'], data=measures)

        columns_ = []

        for i in data_model_raw['model']['tables']:
            try:
                for j in i['columns']:
                    if 'type' in j:
                        Table_name = i['name']
                        column_name = j['name']
                        column_type = j['type']
                        column_expression = j.get('expression', '')
                        columns_.append([report_name, Table_name, column_name, column_type, column_expression])
            except KeyError:
                pass

        columns_df = pd.DataFrame(columns=['Report Name', 'Table Name', 'Column_Name', 'Column_Type', 'Column_Expression'], data=columns_)

        relationship = []

        for i in data_model_raw['model']['relationships']:
            From_table = i['fromTable']
            From_Column = i['fromColumn']
            To_Table = i['toTable']
            To_Column = i['toColumn']
            is_active = i.get('isActive', "NA")
            relationship.append([report_name, From_table, From_Column, To_Table, To_Column, is_active])

        relationship_df = pd.DataFrame(columns=['Report Name', 'From_table', 'From_Column', 'To_Table', 'To_Column', 'is_active'], data=relationship)

        for s in report_layout['sections']:
            for vc in s['visualContainers']:
                try:
                    query_dict = json.loads(vc['config'])
                    for command in query_dict['singleVisual']['prototypeQuery']['Select']:
                        if 'Measure' in command:
                            name = command['Name'].split('.')[1]
                            table = command['Name'].split('.')[0]
                            fields.append([report_name, s['displayName'], query_dict['name'], table, name, 'Measure'])
                        elif 'Column' in command:
                            name = command['Name'].split('.')[1]
                            table = command['Name'].split('.')[0]
                            fields.append([report_name, s['displayName'], query_dict['name'], table, name, 'Column'])
                        elif 'Aggregation' in command:
                            if '(' in command['Name'] and ')' in command['Name']:
                                txt_extraction = command['Name'][command['Name'].find('(') + 1: command['Name'].find(')')]
                                if '.' in txt_extraction:
                                    name = txt_extraction.split('.')[1]
                                    table = txt_extraction.split('.')[0]
                                    fields.append([report_name, s['displayName'], query_dict['name'], table, name, 'Aggregation'])
                except KeyError:
                    pass

        fields_df = pd.DataFrame(columns=['Report Name', 'Page', 'Visual ID', 'Table', 'Name', 'Type'], data=fields)

        shutil.rmtree(pathFolder)

        return data_model_df, measures_df, relationship_df, fields_df, columns_df

def main():
    st.title("Power BI Template (.pbit) Extractor")

    uploaded_files = st.file_uploader("Choose .pbit files", type="pbit", accept_multiple_files=True)

    if uploaded_files is not None:
        data_model_final = pd.DataFrame()
        measures_final = pd.DataFrame()
        relationship_final = pd.DataFrame()
        fields_final = pd.DataFrame()
        columns_final = pd.DataFrame()

        for uploaded_file in uploaded_files:
            file_name = uploaded_file.name
            with open(f"{file_name}", "wb") as f:
                f.write(uploaded_file.getbuffer())

            extractor = ReportExtractor(".", f"{file_name}")
            data_model_df, measures_df, relationship_df, fields_df, columns_df = extractor.extract()

            data_model_final = pd.concat([data_model_final, data_model_df], ignore_index=True)
            measures_final = pd.concat([measures_final, measures_df], ignore_index=True)
            relationship_final = pd.concat([relationship_final, relationship_df], ignore_index=True)
            fields_final = pd.concat([fields_final, fields_df], ignore_index=True)
            columns_final = pd.concat([columns_final, columns_df], ignore_index=True)

        st.success("Files processed successfully!")

        tab1, tab2, tab3, tab4, tab5 = st.tabs(["Data Model", "Measures", "Relationships", "Fields", "Columns"])

        with tab1:
            st.header("Data Model")
            st.dataframe(data_model_final)
            csv = data_model_final.to_csv(index=False).encode('utf-8')
            st.download_button(label="Download Data Model as CSV", data=csv, file_name='data_model.csv', mime='text/csv')

        with tab2:
            st.header("Measures")
            st.dataframe(measures_final)
            csv = measures_final.to_csv(index=False).encode('utf-8')
            st.download_button(label="Download Measures as CSV", data=csv, file_name='measures.csv', mime='text/csv')

        with tab3:
            st.header("Relationships")
            st.dataframe(relationship_final)
            csv = relationship_final.to_csv(index=False).encode('utf-8')
            st.download_button(label="Download Relationships as CSV", data=csv, file_name='relationships.csv', mime='text/csv')

        with tab4:
            st.header("Fields")
            st.dataframe(fields_final)
            csv = fields_final.to_csv(index=False).encode('utf-8')
            st.download_button(label="Download Fields as CSV", data=csv, file_name='fields.csv', mime='text/csv')

        with tab5:
            st.header("Columns")
            st.dataframe(columns_final)
            csv = columns_final.to_csv(index=False).encode('utf-8')
            st.download_button(label="Download Columns as CSV", data=csv, file_name='columns.csv', mime='text/csv')

    st.markdown("""
        <br><br>
        <footer>
        <div style="text-align: center;">
            Developed by <a href="https://www.linkedin.com/in/shrivastavaishan/" target="_blank">Ishan Shrivastava</a>
        </div>
        </footer>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()