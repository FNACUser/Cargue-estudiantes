"""
Created on Thu Jun  6 08:13:08 2024

@author: luis.caro
"""
import datetime
import json
# import pyodbc
import copy
# import itertools as itertools
import pandas as pd
# import numpy as np

from sqlalchemy.orm import close_all_sessions

# from neo4j_connection_sandbox import conn, insert_data
from neo4j_connection_local import conn, insert_data

#Hay que poner a apuntar esto a la BD Finac/Colegio
from sqlalchemy_pure_connection_cloud import session_scope

from new_db_schema import (User, IRA_Nodes_segments_categories, IRA_Nodes,
                            IRA_Nodes_segments, IRA_Organization_areas,
                            IRA_Cycles, IRA_Responses, IRA_Adjacency_input_form,
                            # IRA_Networks_modes_themes,
                            IRA_Networks_modes, IRA_Questions_possible_answers,
                            IRA_Questions, questions_vs_networks_modes,
                            IRA_Employees_interactions)

from monitor_forms_utilities import (FD_Questions_possible_answers,
                                        # function_source
                                        )


#Por alguna razón eso no funciona:
# from Utilities import UT_CountOcurrences, UT_Datetime_to_string
#Tocó hacerlo así:
# from Utilities import UT_CountOcurrences
def UT_Datetime_to_string(datetime_date, xstring="%Y-%m-%d %H:%M:%S"):
    """sumary_line"""
    return datetime.datetime.strftime(datetime_date, xstring)

#%%
# para probar
# query = """MATCH (n) RETURN n"""
# result = conn.query(query)
# result

def FD_delete_constraints():
    """Función encargada de eliminar todos los nodos y relaciones en la DB"""
    # query = """CALL {MATCH (n) DETACH DELETE n} IN TRANSACTIONS"""
    query = """MATCH (n) DETACH DELETE n"""

    result = conn.query(query)

    #for local
    # const = conn.query("CALL db.constraints")
    #for sandbox
    const = conn.query("SHOW CONSTRAINTS")

    for c in const:
        conn.query(f"DROP CONSTRAINT {c['name']}")

FD_delete_constraints()

#forma de hacerlo 1 a 1 parece que no se necesita
# query = """MATCH (nsc:Node_segment_category) DETACH DELETE nsc"""
# result = conn.query(query)
# query = """MATCH (ns:Node_segment) DETACH DELETE ns"""
# result = conn.query(query)
# query = """MATCH (n:Node) DETACH DELETE n"""
# result = conn.query(query)
# query = """MATCH (e:Employee) DETACH DELETE e"""
# result = conn.query(query)
# query = """MATCH (oa:Organization_area) DETACH DELETE oa"""
# result = conn.query(query)
# query = """MATCH (nwm:Network_mode) DETACH DELETE nwm"""
# result = conn.query(query)
# query = """MATCH (nwmt:Network_mode_theme) DETACH DELETE nwmt"""
# result = conn.query(query)
# query = """MATCH (q:Question) DETACH DELETE q"""
# result = conn.query(query)
# query = """MATCH (aif:Adjacency_input_form) DETACH DELETE aif"""
# result = conn.query(query)
# query = """MATCH (c:Cycle) DETACH DELETE c"""
# result = conn.query(query)
# query = """MATCH (r:Response) DETACH DELETE r"""
# result = conn.query(query)
# query = """MATCH (rp:Response_pattern) DETACH DELETE rp"""
# result = conn.query(query)
# query = """MATCH (rp:Response_pattern) DETACH DELETE rp"""
# result = conn.query(query)
# query = """MATCH (rpi:Response_pattern_item) DETACH DELETE rpi"""
# result = conn.query(query)


#.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.- for sandbox
conn.query('CREATE CONSTRAINT nodes_segments_categories ' + \
        'IF NOT EXISTS FOR (e:Node_segment_category) REQUIRE e.id_category IS UNIQUE')
conn.query('CREATE CONSTRAINT nodes_segments ' + \
        'IF NOT EXISTS FOR (k:Node_segment) REQUIRE k.id_segment IS UNIQUE')
conn.query('CREATE CONSTRAINT nodes ' + \
        'IF NOT EXISTS FOR (p:Node) REQUIRE p.id_node IS UNIQUE')
conn.query('CREATE CONSTRAINT employees ' + \
        'IF NOT EXISTS FOR (u:Employee) REQUIRE u.redmine_login IS UNIQUE')
conn.query('CREATE CONSTRAINT employees ' + \
        'IF NOT EXISTS FOR (u:Employee) REQUIRE u.id_employee IS UNIQUE')
conn.query('CREATE CONSTRAINT organization_areas ' + \
        'IF NOT EXISTS FOR (a:Organization_area) REQUIRE a.id_organization_area IS UNIQUE')
conn.query('CREATE CONSTRAINT network_mode ' + \
        'IF NOT EXISTS FOR (a:Network_mode) REQUIRE a.id_network_mode IS UNIQUE')
conn.query('CREATE CONSTRAINT network_mode_theme ' + \
        'IF NOT EXISTS FOR (a:Network_mode_theme) REQUIRE a.id_network_mode_theme '+\
        ' IS UNIQUE')
conn.query('CREATE CONSTRAINT question ' + \
        'IF NOT EXISTS FOR (a:Question) REQUIRE a.id_question IS UNIQUE')
conn.query('CREATE CONSTRAINT adjacency_input_form ' + \
        'IF NOT EXISTS FOR (a:Adjacency_input_form) REQUIRE a.id_adjacency_input_form '+\
        'IS UNIQUE')
conn.query('CREATE CONSTRAINT cycle ' + \
        'IF NOT EXISTS FOR (a:Cycle) REQUIRE a.id_cycle IS UNIQUE')
conn.query('CREATE CONSTRAINT response ' + \
        'IF NOT EXISTS FOR (a:Response) REQUIRE a.id_response IS UNIQUE')
conn.query('CREATE CONSTRAINT response_pattern ' + \
        'IF NOT EXISTS FOR (a:Response_pattern) REQUIRE a.id_response_pattern IS UNIQUE')
conn.query('CREATE CONSTRAINT response_pattern_item ' + \
        'IF NOT EXISTS FOR (a:Response_pattern_item) REQUIRE a.id_response_pattern_item IS UNIQUE')
conn.query('CREATE CONSTRAINT response_text ' + \
        'IF NOT EXISTS FOR (a:Response_text) REQUIRE a.id_response_text IS UNIQUE')

#%%
# cargue de nodos, categorías de nodos, y segmentos
# cargue de nodos, categorías de nodos, y segmentos
#cargue de nodos, categorías de nodos, y segmentos

#nodes son una tripleta de categoría, segmento y nodo
#las categorías y segmentos son: - conocimiento - desarrollo
#                                               - negocio
#                                               - producto
#                                               - profesional
#                                               - técnico
#                                - recurso - capacitación
#                                          - información
#                                          - software
#                                - tarea - fábrica
#                                        - fábricaND
#                                        - producto
#                                        - profesional
#                                        - servicio técnico
#los nodos son conocimientos, recursos y tareas
with session_scope() as session:
    nodes = (session.query(IRA_Nodes_segments_categories, IRA_Nodes_segments,IRA_Nodes)
            .filter(IRA_Nodes_segments_categories.id_node_segment_category ==
                    IRA_Nodes_segments.id_node_segment_category,
                    IRA_Nodes_segments.id_node_segment == IRA_Nodes.id_node_segment)
            .all())

    nodes
    len(nodes)

    nodes_tuples = [(c.id_node_segment_category, c.Node_segment_category,
                        s.id_node_segment, s.Node_segment,
                        n.id_node, n.Node_es, n.Node_en) for c,s,n in nodes]

nodes_tuples

close_all_sessions()

#para relacionar node-segments con network modes
with session_scope() as session:
    nodes_segments = session.query(IRA_Nodes_segments).all()
    nwms = [(ns.id_node_segment, ns.networks_modes)  for ns in nodes_segments]

nodes_segments_network_modes_tuples = [
    (ns,nwm[0].id_network_mode) for ns, nwm in nwms if len(nwm)>0]
nodes_segments_network_modes_tuples

nodes_segments_network_modes_tuples_df = pd.DataFrame(
    nodes_segments_network_modes_tuples, columns=['source','target'])

#:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_ Fin Para probar

categories = [(id_node_segment_category, Node_segment_category)
                for id_node_segment_category, Node_segment_category,_,_,_,_,_
                    in nodes_tuples]

categories = list(set(categories))

categories
categories_df = pd.DataFrame(categories, columns=['id_category','category'])
categories_df

segments_categories = [
    (id_node_segment_category, Node_segment_category, id_node_segment, Node_segment)
    for id_node_segment_category, Node_segment_category, id_node_segment, Node_segment,_,_,_
    in nodes_tuples]

segments = [
    (id_node_segment, Node_segment) for _, _, id_node_segment, Node_segment, _,_,_ in nodes_tuples]

segments_categories = list(set(segments_categories))
segments_categories

segments = [(id_node_segment, Node_segment) for _, _, id_node_segment, Node_segment
            in segments_categories]
segments_df = pd.DataFrame(segments, columns=['id_segment','segment'])
segments_df

link_segments_categories = [(id_node_segment, id_node_segment_category )
                            for id_node_segment_category, _, id_node_segment, _
                                in segments_categories]
link_segments_categories_df = pd.DataFrame(link_segments_categories,
                                            columns = ['source','target'])
link_segments_categories_df

nodes_segments = [(id_node_segment, Node_segment, id_node, Node)
                    for _, _, id_node_segment, Node_segment, id_node, Node, _
                        in nodes_tuples]
nodes_segments = list(set(nodes_segments))
nodes_segments

nodes = [(id_node, Node) for _, _, id_node, Node in nodes_segments]
nodes_df = pd.DataFrame(nodes, columns=['id_node','node'])
nodes_df

link_nodes_segments = [(id_node, id_node_segment )
                        for id_node_segment, _, id_node, _ in nodes_segments]
link_nodes_segments_df = pd.DataFrame(link_nodes_segments,
                                        columns = ['source','target'])
link_nodes_segments_df

def add_nodes_segments_categories(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MERGE (:Node_segment_category {id_category: row.id_category,
                                                category: row.category})
                RETURN COUNT(*) AS total
            """
    return insert_data(query, rows)
add_nodes_segments_categories(categories_df)
# categories_df


def add_nodes_segments(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MERGE (:Node_segment {id_segment: row.id_segment, segment: row.segment})
                RETURN COUNT(*) AS total
            """
    return insert_data(query, rows)
add_nodes_segments(segments_df)

def add_nodes(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MERGE (:Node {id_node: row.id_node, node: row.node})
                RETURN COUNT(*) AS total
            """
    return insert_data(query, rows)
add_nodes(nodes_df)

def add_nodes_segments_categories_rel(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MATCH (source:Node_segment {id_segment: row.source})
                MATCH (target:Node_segment_category {id_category: row.target})
                MERGE (source)-[r:ES_DE_CATEGORIA]->(target)
                RETURN COUNT(r) AS total
            """
    return insert_data(query, rows)
add_nodes_segments_categories_rel(link_segments_categories_df)

def add_nodes_nodes_segments_rel(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MATCH (source:Node {id_node: row.source})
                MATCH (target:Node_segment {id_segment: row.target})
                MERGE (source)-[r:ES_DE_SEGMENTO]->(target)
                RETURN COUNT(r) AS total
            """
    return insert_data(query, rows)
add_nodes_nodes_segments_rel(link_nodes_segments_df)


close_all_sessions()

#_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:
#%% FIN cargue de nodos, categorías de nodos, y segmentos
#%% :_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:


#%%
#cargue de employees y areas
#cargue de employees y areas
#cargue de employees y areas

#las áreas son: - Producto y certificaciones
#               - Soporte e infraestructura
#               - Proyectos
#               - Servicio al cliente
#               - Desarrollo
#               - Administración
#               - Consultoría
#               - Comercial

#.-.-.-.-.-.-.-.-.-.-.-.-.-.-..-.-.-.-.-.-
with session_scope() as session:
    queried_employees = session.query(User).all()
    queried_organization_areas = session.query(IRA_Organization_areas).all()
    employees = copy.deepcopy(queried_employees)
    employees_tuple = [(employee.id, employee.username, employee.id_redmine,
                        employee.id_organization_area) for employee in employees]
    organization_areas = copy.deepcopy(queried_organization_areas)
    organization_areas_tuple = [(organization_area.id_organization_area,
                                organization_area.Organization_area_es)
                                for organization_area in organization_areas]
    session.commit()
# \
#                                 if employee.id_redmine not in \
#                                     ['evangelica.delgado',
#                                      'luz.ortiz',None]]
len(employees_tuple)
employees_df = pd.DataFrame(
    employees_tuple, columns=['id_employee','t_employee', 'redmine_user',
                                'id_organization_area'])

len(organization_areas_tuple)
organization_areas_df = pd.DataFrame(
    organization_areas_tuple, columns=['id_organization_area', 'organization_area'])

organization_areas_df.drop_duplicates(keep='first', inplace=True, ignore_index=True)

# def FD_complete_employees(xusers, xemployees_df):

#     def change_user_date(xdate):
#         print('xdate')
#         print(xdate)
#         return UT_Datetime_to_string(xdate,xstring="%Y-%m-%d")

#     # def find_employee_in_redmine(xredmine_user):
#     #     user = xusers.loc[xusers.login == xredmine_user]
#     #     return user

#     def redmine_id(xrow):

#         # employee_in_redmine_df = \
#         #     find_employee_in_redmine(xrow['redmine_user'])
#         # if employee_in_redmine_df.shape[0] == 0:
#         redmine_id = xrow['id_employee']+1000
#         # else:
#         #     redmine_id = list(employee_in_redmine_df['id'])[0]
#         return redmine_id

#     def complete_employee(xrow):

#         # employee_in_redmine_df = \
#         #     find_employee_in_redmine(xrow['redmine_user'])
#         # if employee_in_redmine_df.shape[0] == 0:
#         employee = xrow['t_employee']
#         # else:
#         #     employee = list(employee_in_redmine_df['user_name'])[0]
#         return employee

#     def created_on(xrow):

#         # employee_in_redmine_df = \
#         #     find_employee_in_redmine(xrow['redmine_user'])
#         # if employee_in_redmine_df.shape[0] == 0:
#         created_on = '2022-01-01'
#         # else:
#         #     created_on = \
#         #         change_user_date(list(employee_in_redmine_df['created_on'])[0])
#         return created_on

#     xemployees_df['redmine_id'] =\
#         xemployees_df.apply(redmine_id, axis=1)
#     xemployees_df['employee'] =\
#         xemployees_df.apply(complete_employee, axis=1)
#     xemployees_df['created_on'] =\
#         xemployees_df.apply(created_on, axis=1)

# #Aquí era el problema con Redmine
# FD_complete_employees(redmine_users, employees_df)

employees_df.rename(columns={'t_employee': 'employee'} ,inplace=True)
employees_df.columns

employees_df = employees_df.merge(
    organization_areas_df, left_on='id_organization_area',
    right_on='id_organization_area', how='inner')
employees_df['is_active'] = True
employees_df['created_on'] = created_on = '2023-06-30'
employees_df['redmine_id'] = employees_df['redmine_user']


def add_employees(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MERGE (e:Employee {id_employee: row.id_employee,
                                    redmine_id: row.redmine_id,
                                    redmine_login: row.redmine_user,
                                    employee: row.employee,
                                    created_on: row.created_on})
                SET e.is_active = row.is_active
                RETURN COUNT(*) AS total
            """
    return insert_data(query, rows)
add_employees(employees_df)

# SET e.is_active: row.is_active

def add_areas(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MERGE (:Organization_area {id_organization_area:
                                            row.id_organization_area,
                                            organization_area:
                                            row.organization_area})
                RETURN COUNT(*) AS total
            """
    return insert_data(query, rows)
add_areas(organization_areas_df)

def add_employees_organization_areas_rel(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MATCH (source:Employee {redmine_login: row.redmine_user})
                MATCH (target:Organization_area {id_organization_area:
                                                row.id_organization_area})
                MERGE (source)-[r:FUNCIONARIO_DE]->(target)
                RETURN COUNT(r) AS total
            """
    return insert_data(query, rows)
add_employees_organization_areas_rel(employees_df)


#OJO.-.-.-.-.- esto toca correrlo
query = """MATCH (e:Employee)-[r:FUNCIONARIO_DE]->(a:Organization_area)
            SET e.organization_area = a.organization_area
        """
conn.query(query)

close_all_sessions()

#_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:
#%% FIN cargue de employees y areas
#%% :_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:

#%% Relación de nodos creados por empleados al contestar

with session_scope() as session:
    nodes = session.query(IRA_Nodes).all()

    nodes
    len(nodes)

nodes_tuples = [(n.id_node, n.Node_es, n.id_employee) for n in nodes
                if n.id_employee is not None]
nodes_tuples
nodes_from_employees_df = pd.DataFrame(
    nodes_tuples, columns=['id_node','name', 'id_employee'])
nodes_from_employees_df

def add_nodes_employees_rel(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MATCH (source:Node {id_node: row.id_node})
                MATCH (target:Employee {id_employee: row.id_employee})
                MERGE (source)-[r:NODE_FROM_EMPLOYEE]->(target)
                RETURN COUNT(r) AS total
            """
    return insert_data(query, rows)
add_nodes_employees_rel(nodes_from_employees_df)

#%%
# cargue de Cycles
# cargue de Cycles
# cargue de Cycles

def FD_add_cycles():
    """sumary_line"""
    def add_cycles(rows):
        """sumary_line"""
        query = """UNWIND $rows AS row
                    MERGE (:Cycle {id_cycle: row.id_cycle,
                                    initial_date: date(row.initial_date),
                                    end_date: date(row.end_date)})
                    RETURN COUNT(*) AS total
                """
        return insert_data(query, rows)

    def change_date(xdate):
        """sumary_line"""
        return UT_Datetime_to_string(xdate,xstring="%Y-%m-%d")

    with session_scope() as session:
        cycles = session.query(IRA_Cycles).all()
        cycles_df = pd.DataFrame(
            [(c.id_cycle, c.Initial_date, c.End_date) for c in cycles],
            columns=['id_cycle', 'Initial_date', 'End_date'])

    cycles_df['initial_date'] = cycles_df['Initial_date'].apply(change_date)
    cycles_df['end_date'] = cycles_df['End_date'].apply(change_date)
    cycles_df.drop(['Initial_date','End_date'],axis=1,inplace=True)

    add_cycles(cycles_df)

    return cycles_df

a=FD_add_cycles()

#_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:
#%% FIN cargue de cycles
#%% :_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:


#%% cargue de response_patterns y response_patterns_items
#%% cargue de response_patterns y response_patterns_items
#%% cargue de response_patterns y response_patterns_items

with session_scope() as session:
    questions_possible_answers = session.query(IRA_Questions_possible_answers).all()

    (questions_possible_answers_patterns_dict, questions_multiple_option_indicator_dict) = (
        FD_Questions_possible_answers(questions_possible_answers))

#.-.-.-.-.-.-.-.-.-.-.-.-.-.- sacado para probar
# response_patterns_dict = \
#     {k:str(k) for k,_ in questions_possible_answers_patterns_dict.items()}
# questions_possible_answers_patterns_dict

# response_patterns_df = \
#     pd.DataFrame({'response_pattern': response_patterns_dict})

# response_patterns_items_list = \
#     [(str(k), str(k) + '-' + _k, int(_k), _v) \
#      for k,v in questions_possible_answers_patterns_dict.items()\
#          for _k,_v in v.items()]

# response_patterns_items_df = \
#     pd.DataFrame(response_patterns_items_list,
#                  columns=['response_pattern_id',
#                           'response_pattern_item_id',
#                           'response_pattern_item_value',
#                           'response_pattern_item_meaning'])

# def add_response_patterns(rows):
#     query = """UNWIND $rows AS row
#                 MERGE (:Response_pattern {id_response_pattern: \
#                                           row.response_pattern})
#                 RETURN COUNT(*) AS total
#             """
#     return insert_data(query, rows)
# add_response_patterns(response_patterns_df)

# def add_response_patterns_items(rows):
#     query = """UNWIND $rows AS row
#                 MERGE (:Response_pattern_item \
#                         {id_response_pattern_item: \
#                         row.response_pattern_item_id,\
#                             item_value: row.response_pattern_item_value,\
#                                 item_meaning: \
#                                     row.response_pattern_item_meaning})
#                 RETURN COUNT(*) AS total
#             """
#     return insert_data(query, rows)
# add_response_patterns_items(response_patterns_items_df)

# def add_response_patterns_patterns_items_rel(rows):
#     query = """UNWIND $rows AS row
#                 MATCH (source:Response_pattern_item \
#                         {id_response_pattern_item: \
#                         row.response_pattern_item_id})
#                 MATCH (target:Response_pattern {id_response_pattern: \
#                                                 row.response_pattern_id})
#                 MERGE (source)-[r:RESPONSE_ITEM_OF]->(target)
#                 RETURN COUNT(r) AS total
#             """
#     return insert_data(query, rows)
# add_response_patterns_patterns_items_rel(response_patterns_items_df)

#:_:_:_:_:_:_:_:_:_:_:_:_:_: fin sacado

def FD_response_patterns_GDB(xquestions_possible_answers_dict):
    """sumary_line"""
    response_patterns_dict = {
        k:str(k) for k,_ in xquestions_possible_answers_dict.items()}

    response_patterns_df = pd.DataFrame({'response_pattern': response_patterns_dict})

    response_patterns_items_list = [
        (str(k), str(k) + '-' + _k, int(_k), _v)
        for k,v in xquestions_possible_answers_dict.items() for _k,_v in v.items()]

    response_patterns_items_df = pd.DataFrame(
        response_patterns_items_list,
        columns=['response_pattern_id', 'response_pattern_item_id',
                    'response_pattern_item_value', 'response_pattern_item_meaning'])

    def add_response_patterns(rows):
        """sumary_line"""
        query = """UNWIND $rows AS row
                    MERGE (:Response_pattern {id_response_pattern: row.response_pattern})
                    RETURN COUNT(*) AS total
                """
        return insert_data(query, rows)
    add_response_patterns(response_patterns_df)

    def add_response_patterns_items(rows):
        """sumary_line"""
        query = """UNWIND $rows AS row
                    MERGE (:Response_pattern_item
                            {id_response_pattern_item: row.response_pattern_item_id,
                                item_value: row.response_pattern_item_value,
                                item_meaning: row.response_pattern_item_meaning})
                    RETURN COUNT(*) AS total
                """
        return insert_data(query, rows)
    add_response_patterns_items(response_patterns_items_df)

    def add_response_patterns_patterns_items_rel(rows):
        """sumary_line"""
        query = """UNWIND $rows AS row
                    MATCH (source:Response_pattern_item
                            {id_response_pattern_item: row.response_pattern_item_id})
                    MATCH (target:Response_pattern
                            {id_response_pattern: row.response_pattern_id})
                    MERGE (source)-[r:RESPONSE_ITEM_OF]->(target)
                    RETURN COUNT(r) AS total
                """
        return insert_data(query, rows)
    add_response_patterns_patterns_items_rel(response_patterns_items_df)

    return response_patterns_df, response_patterns_items_df

a, b = FD_response_patterns_GDB(questions_possible_answers_patterns_dict)

close_all_sessions()



#_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:
#%% FIN cargue de response_patterns y response_patterns_items
#%% :_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:_:

# r = """[{\"item_id\": 1, \"valor\": [560, 562, 558, 567, 566]},
#         {\"item_id\": 1, \"valor\": [566, 568, 569]}]"""
# json.loads(r)

#%%
# employes vs nodes
# employes vs nodes
# employes vs nodes

#import from funciones_leer_json_parrafos
def get_values_from_dict(xrtas_pregunta,xfield):
    """sumary_line"""
    # print('.-.-.-.-.-.-.- get_values_from_dict')
    # print('>>>>>>>>>>>>>>>>> xrtas_pregunta (get_values_from_dict)')
    # print(xrtas_pregunta)
    # print('>>>>>>>>>>>>>>>>> xfield (get_values_from_dict)')
    # print(xfield)
    rtas_pregunta = json.loads (xrtas_pregunta)
    # print('>>>>>>>>>>>>>>>>> rtas_pregunta (get_values_from_dict)')
    # print(rtas_pregunta)
    values = [str(rtas_pregunta[j][xfield])  for j in  range(len(rtas_pregunta))]
    # print('>>>>>>>>>>>>>>>>> values (get_values_from_dict)')
    # print(values)

    return values


def FD_fetch_Responses(xsession, xid_adjacency_input_form):
    """sumary_line"""
    # with session_scope() as session:
    responses = (xsession.query(IRA_Responses)
                .filter(IRA_Responses.id_adjacency_input_form ==
                        xid_adjacency_input_form).all())

    return responses

# FD_fetch_Responses('1-1-1')

def FD_fetch_Adjacency_input_form(xsession, xid_employee, xid_network_mode, xid_cycle):
    """sumary_line"""
    print('.-.-.-.-.-.-.-.-.-.- FD_fetch_Adjacency_input_form')

    def add_adjacency_input_form(rows):
        """sumary_line"""
        query = """UNWIND $rows AS row
                    MERGE (:Adjacency_input_form {id_adjacency_input_form:
                                                    row.id_adjacency_input_form})
                    RETURN COUNT(*) AS total
                """
        return insert_data(query, rows)

    def add_adjacency_input_form_employee_rel(rows):
        """sumary_line"""
        query = """UNWIND $rows AS row
                    MATCH (source:Adjacency_input_form {id_adjacency_input_form:
                                                    row.id_adjacency_input_form})
                    MATCH (target:Employee {id_employee: row.id_employee})
                    MERGE (source)-[r:OF_EMPLOYEE]->(target)
                    RETURN COUNT(r) AS total
                """
        return insert_data(query, rows)

    def add_adjacency_input_form_network_mode_rel(rows):
        """sumary_line"""
        query = """UNWIND $rows AS row
                    MATCH (source:Adjacency_input_form {id_adjacency_input_form:
                                                    row.id_adjacency_input_form})
                    MATCH (target:Network_mode {id_network_mode: row.id_network_mode})
                    MERGE (source)-[r:OF_NETWORK_MODE]->(target)
                    RETURN COUNT(r) AS total
                """
        return insert_data(query, rows)

    def add_adjacency_input_form_cycle_rel(rows):
        """sumary_line"""
        query = """UNWIND $rows AS row
                    MATCH (source:Adjacency_input_form {id_adjacency_input_form:
                                                        row.id_adjacency_input_form})
                    MATCH (target:Cycle {id_cycle: row.id_cycle})
                    MERGE (source)-[r:OF_CYCLE]->(target)
                    RETURN COUNT(r) AS total
                """
        return insert_data(query, rows)

    id_adjacency_input_form = \
        str(xid_cycle)+'-'+str(xid_employee)+'-'+str(xid_network_mode)

    print('>>>>>>>>>>>>>>>> id_adjacency_input_form (FD_fetch_Adjacency_input_form)')
    print(id_adjacency_input_form)

    # with session_scope() as session:
    adjacency_input_form = (xsession.query(IRA_Adjacency_input_form)
                            .filter(IRA_Adjacency_input_form.id_adjacency_input_form ==
                                    id_adjacency_input_form).first())
    #hasta la session

    print('>>>>>>>>>>>>>>>> adjacency_input_form (FD_fetch_Adjacency_input_form)')
    print(adjacency_input_form)

    if adjacency_input_form != None:
        adjacency_input_form_df = pd.DataFrame(
            [(aif.id_adjacency_input_form, aif.id_employee, aif.id_network_mode,
                aif.id_cycle) for aif in [adjacency_input_form]],
            columns=['id_adjacency_input_form', 'id_employee', 'id_network_mode',
                        'id_cycle'])

        print('>>>>>> adjacency_input_form_df (FD_fetch_Adjacency_input_form)')
        print(adjacency_input_form_df.to_dict('records'))

        #Volver a poner - es para grabar en neo4j
        add_adjacency_input_form(adjacency_input_form_df)

        add_adjacency_input_form_employee_rel(adjacency_input_form_df)

        add_adjacency_input_form_network_mode_rel(adjacency_input_form_df)

        add_adjacency_input_form_cycle_rel(adjacency_input_form_df)

    responses = FD_fetch_Responses(xsession, id_adjacency_input_form)

    # print('>>>>>>>>>>>>>>>>>>> responses (FD_fetch_Adjacency_input_form)')
    # print(responses)

    return id_adjacency_input_form, responses


#.-.-.-.-.-.-.-
def FD_employee_node_rel(xsession, xquestion, xid_employee, xid_user, xid_network_mode,
                            xnetwork_mode, xquestions_edge_name_dict, xid_cycle,
                            xactor_modes):
    """sumary_line"""
    print('.-.-.-.-.-.-.-.-.-FD_employee_node_rel')
    # print('.-.-.-.-.-.-.-.-.- xquestion.id (FD_employee_node_rel)')
    # print(xquestion.id_question)
    # print('.-.-.-.-.-.-.-.-.- xquestion (FD_employee_node_rel)')
    # print(xquestion)
    # print('.-.-.-.-.-.-.-.-.- xnetwork_mode (FD_employee_node_rel)')
    # print(xnetwork_mode)
    # print('.-.-.-.-.-.-.-.-.- xid_employee (FD_employee_node_rel)')
    # print(xid_employee)
    # print('.-.-.-.-.-.-.-.-.- xid_user (FD_employee_node_rel)')
    # print(xid_user)

    print('>>>>>>>> id_employee')
    print(xid_employee)

    id_adjacency_input_form, responses = (
        FD_fetch_Adjacency_input_form(xsession, xid_employee, xid_network_mode,
                                        xid_cycle))
    # print('>>>>>>>>>>> id_adjacency_input_form (FD_employee_node_rel))')
    # print(id_adjacency_input_form)
    # print('>>>>>>>>>>> responses (FD_employee_node_rel))')
    # print(responses)

    response_json = [response.Response for response in responses
                        if response.id_question == xquestion.id_question]

    # print('>>>>>>>>>>> response_json (FD_employee_node_rel))')
    # print(response_json)
    # print(len(response_json))

    if len(response_json) > 0:
        valores_str = get_values_from_dict(response_json[0],'valor')

        valores = [valor_str for valor_str in valores_str]
        # print('>>>>>>>>>>> valores (FD_employee_node_rel))')
        # print(valores)

        ids_nodos_str = get_values_from_dict(response_json[0],'item_id')
        ids_nodos = [int(id_nodo_str) for id_nodo_str in ids_nodos_str]
        # print('>>>>>>>>>>> ids_nodos (FD_employee_node_rel))')
        # print(ids_nodos)

        id_response_prefix = str(xid_employee) + '_' + id_adjacency_input_form + \
            '_' + str(xquestion.id_question) + '_' + str(xid_network_mode) + '_'
        # print('>>>>>>>>>>> id_response_prefix (FD_employee_node_rel))')
        # print(id_response_prefix)

        id_response = [id_response_prefix + str(id_nodo) for id_nodo in ids_nodos]
        # print('>>>>>>>>>>> id_response (FD_employee_node_rel))')
        # print(id_response)

        relationship_dict ={
            'id_response': id_response,
            'id_adjacency_input_form': [id_adjacency_input_form]*len(ids_nodos),
            'source':[xid_user]*len(ids_nodos),
            'target':ids_nodos,
            'id_question':[xquestion.id_question]*len(ids_nodos),
            'pregunta':[xquestion.Question_es]*len(ids_nodos),
            'value':valores}
        # print('>>>>>>>>>>> relationship_dict (FD_employee_node_rel))')
        # print(relationship_dict)

        relationship_df = pd.DataFrame(
            relationship_dict,
            columns=['id_response', 'id_adjacency_input_form', 'source', 'target',
                        'id_question', 'pregunta', 'value'])

        # print('>>>>>>>>>>> relationship_df (FD_employee_node_rel))')
        # print(relationship_df)

        # print(UT_CountOcurrences(relationship_df,['value']))

        relationship_df = relationship_df.loc[(relationship_df.value != '-') &
                                                (relationship_df.value != '')]
        # print('>>>>>>>>>>>> realtionship_df (FD_employee_node_rel)')
        # print(relationship_df.to_dict('records'))
        # print(UT_CountOcurrences(relationship_df,['value']))

        if xid_network_mode in xactor_modes:
            target = 'Employee'
        else:
            target = 'Node'

        #Volver a poner es para grabar en neo4j
        # add_responses_rel(relationship_df, target)

def FD_employees_nodes_rel(xsession, xquestion, xemployees, xid_network_mode,
                            xnetwork_mode, xquestions_edge_name_dict,
                            xid_cycle, xactor_modes):

    print('.-.-.-.-.-.-.-.-.- FD_employees_nodes_rel')
    # print('>>>>>>>>>>>>>>>>> xemployees[0:3] (FD_employees_nodes_rel)')
    # print(xemployees[0:3])

    [FD_employee_node_rel(xsession, xquestion, employee.id, employee.id_redmine,
                            xid_network_mode, xnetwork_mode,
                            xquestions_edge_name_dict, xid_cycle, xactor_modes)
        for employee in xemployees]

def FD_employees_modes_nodes_rel(xsession, xid_network_mode, xemployees,
                                    xquestions_edge_name_dict,
                                    xid_cycle, xactor_modes):
    """sumary_line"""
    print('.-.-.-.-.-.-.-.-.- FD_employees_modes_nodes_rel')

    #
    #returns:   - network_mode_df
    #           - networks_modes_themes_df
    #           - questions_df
    # with session_scope() as session:

    query_data = (xsession.query(IRA_Networks_modes)
                    .filter(IRA_Networks_modes.id_network_mode == xid_network_mode)
                    .all())
    data = copy.deepcopy(query_data)
    print('>>>>>>>>>>>>data (FD_employees_modes_nodes_rel)')
    print(data)

    #
    #para estudiantes el id_network_mode_theme es 1, aunque en la base de
    #datos viene vacío
    #Se cambia esta línea: d.id_node_segment, d.id_network_mode_theme)
    network_mode_df = pd.DataFrame(
        [(d.id_network_mode, d.network.name_es, d.id_node_segment, 1)
            for d in data],
        columns=['id_network_mode', 'Network_mode', 'id_node_segment_category',
                    'id_network_mode_theme'])
    print('>>>>>>>>>>>>network_mode_df (FD_employees_modes_nodes_rel)')
    print(network_mode_df)
    network_mode = network_mode_df['Network_mode'][0].lower()
    print('>>>>>>>>>>>>network_mode (FD_employees_modes_nodes_rel)')
    print(network_mode)

    #Para estudiantes no hay networks_modes_themes
    # query_networks_modes_themes = \
    #     session.query(IRA_Networks_modes_themes).all()
    # networks_modes_themes = copy.deepcopy(query_networks_modes_themes)
    # networks_modes_themes_df = \
    #     pd.DataFrame([(d.id_network_mode_theme, d.Network_mode_theme_es) \
    #                   for d in networks_modes_themes],
    #               columns=['id_network_mode_theme', 'network_mode_theme'])
    networks_modes_themes_df = pd.DataFrame(
        [(1, 'sociograma')], columns=['id_network_mode_theme', 'network_mode_theme'])
    print('>>>>>>>networks_modes_themes_df (FD_employees_modes_nodes_rel)')
    print(networks_modes_themes_df)

    query_questions = (session.query(IRA_Networks_modes)
                        .filter(IRA_Networks_modes.id_network_mode == xid_network_mode)
                        .first().questions.all())
    questions = copy.deepcopy(query_questions)
    questions_df = pd.DataFrame(
        [(d.id_question, d.Question_es,  str(d.id_question_possible_answers))
            for d in questions],
        columns=['id_question', 'Question', 'id_question_possible_answers'])
    print('>>>>>>>>>>>> questions_df (FD_employees_modes_nodes_rel)')
    print(questions_df.to_dict('records'))
    #aquí se acaba el session

    def add_network_mode(rows):
        """sumary_line"""
        query = """UNWIND $rows AS row
                    MERGE (:Network_mode {id_network_mode: row.id_network_mode,
                                            network_mode: row.Network_mode})
                    RETURN COUNT(*) AS total
                """
        return insert_data(query, rows)
    add_network_mode(network_mode_df)

    def add_networks_modes_themes(rows):
        """sumary_line"""
        query = """UNWIND $rows AS row
                    MERGE (:Network_mode_theme {id_network_mode_theme:
                                                row.id_network_mode_theme,
                                                network_mode_theme:
                                                    row.network_mode_theme})
                    RETURN COUNT(*) AS total
                """
        return insert_data(query, rows)
    add_networks_modes_themes(networks_modes_themes_df)

    def add_questions(rows):
        """sumary_line"""
        query = """UNWIND $rows AS row
                    MERGE (:Question {id_question: row.id_question,
                                        question: row.Question})
                    RETURN COUNT(*) AS total
                """
        return insert_data(query, rows)
    add_questions(questions_df)

    def add_questions_response_patterns_rel(rows):
        """sumary_line"""
        query = """UNWIND $rows AS row
                    MATCH (source:Question {id_question: row.id_question})
                    MATCH (target:Response_pattern {id_response_pattern:
                                                    row.id_question_possible_answers})
                    MERGE (source)-[r:HAS_PATTERN]->(target)
                    RETURN COUNT(r) AS total
                """
        return insert_data(query, rows)
    add_questions_response_patterns_rel(questions_df)

    questions_df['id_network_mode'] = network_mode_df['id_network_mode'][0]

    def add_network_mode_network_mode_theme_rel(rows):
        """sumary_line"""
        query = """UNWIND $rows AS row
                    MATCH (source:Network_mode {id_network_mode: row.id_network_mode})
                    MATCH (target:Network_mode_theme {id_network_mode_theme:
                                                        row.id_network_mode_theme})
                    MERGE (source)-[r:IS_OF_THEME]->(target)
                    RETURN COUNT(r) AS total
                """
        return insert_data(query, rows)
    add_network_mode_network_mode_theme_rel(network_mode_df)

    def add_network_mode_questions_rel(rows):
        """sumary_line"""
        query = """UNWIND $rows AS row
                    MATCH (source:Question {id_question: row.id_question})
                    MATCH (target:Network_mode {id_network_mode: row.id_network_mode})
                    MERGE (source)-[r:QUESTION_FOR]->(target)
                    RETURN COUNT(r) AS total
                """
        return insert_data(query, rows)
    add_network_mode_questions_rel(questions_df)

    print('VOLVERLO A PONER<<<<<<<<<<<<')
    [FD_employees_nodes_rel(xsession, question, xemployees, xid_network_mode,
                            network_mode, xquestions_edge_name_dict,
                            xid_cycle, xactor_modes)
        for question in questions]

questions_edge_name_dict = {1:'RECIBO', 2:'ENTREGO', 3:'RESUELVO_CON', 4:'CONOZCO',
                            5:'TENGO', 6:'APLICO', 7:'TENGO_ACCESO', 8:'USO',
                            9:'SE_COMO', 10:'EJECUTO'}

employees_sel = [user for user in employees]

#.-.-.-.-.-.-.-.-.-.-.- Finac Mayo 2023
# with session_scope() as session:
#     [FD_employees_modes_nodes_rel(session, id_network_mode, employees_sel,
#                                   questions_edge_name_dict,1,[1,2,3])
#         for id_network_mode in [1,2,3,4,5,6,7,8,9,10]]

with session_scope() as session:
    [FD_employees_modes_nodes_rel(session, id_network_mode, employees_sel,
                                    questions_edge_name_dict,1,[1])
        for id_network_mode in [1]]

def add_nodes_nodes_segments_network_modes_rel(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MATCH (source:Node_segment {id_segment: row.source})
                MATCH (target:Network_mode {id_network_mode: row.target})
                MERGE (source)-[r:SEGMENTO_DE_NETWORK_MODE]->(target)
                RETURN COUNT(r) AS total
            """
    return insert_data(query, rows)
add_nodes_nodes_segments_network_modes_rel(nodes_segments_network_modes_tuples_df)

close_all_sessions()


#%%

with session_scope() as session:
    queried_adjacency_input_form = session.query(IRA_Adjacency_input_form).all()
    queried_questions = session.query(IRA_Questions).all()
    queried_questions_vs_networks_modes_tuples = (
        session.query(questions_vs_networks_modes).all())
    queried_responses = session.query(IRA_Responses).all()
    queried_employees_interactions = (session.query(IRA_Employees_interactions).all())

    adjacency_input_form = copy.deepcopy(queried_adjacency_input_form)
    questions = copy.deepcopy(queried_questions)
    questions_vs_networks_modes_tuples = (
        copy.deepcopy(queried_questions_vs_networks_modes_tuples))
    responses = copy.deepcopy(queried_responses)
    employees_interactions = copy.deepcopy(queried_employees_interactions)

adjacency_input_form_list = [
    (aif.id_adjacency_input_form, aif.id_employee, aif.id_network_mode)
        for aif in adjacency_input_form ]

        # \
        #  if (aif.id_employee != 34) & (aif.id_network_mode in {1})]

        # ,
        #                  columns=['id_adjacency_input_form',
        #                           'id_employee', 'id_network_mode',
        #                           'is_concluded'])

questions_df = pd.DataFrame(
    [(q.id_question, q.Question_es, q.id_question_possible_answers) for q in questions],
    columns=['id_question', 'question', 'id_question_possible_answers'])

questions_vs_networks_modes_df = pd.DataFrame(
    questions_vs_networks_modes_tuples, columns=['id_question', 'id_network_mode'])

responses_df = pd.DataFrame(
    [(r.id_response, r.Response, r.id_question, r.id_adjacency_input_form)
        for r in responses],
    columns=['id_response', 'response', 'id_question', 'id_adjacency_input_form'])
    
employees_interactions_df = pd.DataFrame(
    [(ei.id_responding_employee, ei.id_interacting_employee)
        for ei in employees_interactions],
    columns=['id_responding_employee', 'id_interacting_employee'])


# def FD_insert_responses(xresponse_tuple, xxxactor_network_modes):

#     print('.-.-.-.-.-. alcaparros_estudiantes_load_better/FD_insert_responses')
#     id_adjacency_input_form, id_employee, id_network_mode = xresponse_tuple
#     print('>>>>>>>>>>>>>>>> id_adjacency_input_form (FD_insert_responses)')
#     print(id_adjacency_input_form)
#     print('>>>>>>>>>>>>>>>> id_employee (FD_insert_responses)')
#     print(id_employee)
#     print('>>>>>>>>>>>>>>>> id_network_mode (FD_insert_responses)')
#     print(id_network_mode)

#     #
#     #En estudiantes solo hay id_network_mode = 1
#     if id_network_mode == 1:

#         _, _, adjacency_input_form_display_df = \
#             function_source(id_employee, id_network_mode,
#                             id_adjacency_input_form, questions_df,
#                             questions_vs_networks_modes_df,
#                             responses_df, employees_df, employees_interactions_df,
#                             questions_possible_answers_patterns_dict,
#                             questions_multiple_option_indicator_dict)

#         if adjacency_input_form_display_df.shape[0] > 0:
#             def add_response(rows):
#                 query = """UNWIND $rows AS row
#                             MERGE (:Response {id_response: row.id_response})
#                             RETURN COUNT(*) AS total
#                         """
#                 return insert_data(query, rows)
#             add_response(adjacency_input_form_display_df)

#             def add_responses_questions_rel(rows):
#                 query = """UNWIND $rows AS row
#                             MATCH (source:Response {id_response: row.id_response})
#                             MATCH (target:Question {id_question: row.id_question})
#                             MERGE (source)-[r:FOR_QUESTION]->(target)
#                             RETURN COUNT(r) AS total
#                         """
#                 return insert_data(query, rows)
#             add_responses_questions_rel(adjacency_input_form_display_df)

#             def add_responses_adjacency_input_forms_rel(rows):
#                 query = """UNWIND $rows AS row
#                             MATCH (source: Response {id_response: row.id_response})
#                             MATCH (target: Adjacency_input_form {id_adjacency_input_form:
#                                                           row.id_adjacency_input_form})
#                             MERGE (source)-[r:OF_FORM]->(target)
#                             RETURN COUNT(r) AS total
#                         """
#                 return insert_data(query, rows)
#             add_responses_adjacency_input_forms_rel(adjacency_input_form_display_df)

#             # if id_network_mode in xactor_network_modes:
#             def add_responses_related_employees_rel(rows):
#                 query = """UNWIND $rows AS row
#                     MATCH (source: Response {id_response: row.id_response})
#                     MATCH (target: Employee {id_employee: row.id_interacting_employee})
#                     MERGE (source)-[r:RELATED_TO]->(target)
#                     RETURN COUNT(r) AS total
#                 """
#                 return insert_data(query, rows)
#             add_responses_related_employees_rel(adjacency_input_form_display_df)
#             # else:
#             #     def add_responses_related_nodes_rel(rows):
#             #         query = """UNWIND $rows AS row
#             #                     MATCH (source: Response {id_response: row.id_response})
#             #                     MATCH (target: Node {id_node: row.id_interacting_employee})
#             #                     MERGE (source)-[r:ABOUT]->(target)
#             #                     RETURN COUNT(r) AS total
#             #                 """
#             #         return insert_data(query, rows)
#             #     add_responses_related_nodes_rel(adjacency_input_form_display_df)

#             def add_employees_responses_rel(rows):
#                 query = """UNWIND $rows AS row
#                             MATCH (source: Employee {id_employee: row.id_employee})
#                             MATCH (target: Response {id_response: row.id_response})
#                             MERGE (source)-[r:HAS_RESPONSE]->(target)
#                             RETURN COUNT(r) AS total
#                         """
#                 return insert_data(query, rows)
#             add_employees_responses_rel(adjacency_input_form_display_df)

#             def add_responses_response_pattern_items_rel(rows):
#                 query = """UNWIND $rows AS row
#                             MATCH (source: Response {id_response: row.id_response})
#                             MATCH (target: Response_pattern_item \
#                                    {id_response_pattern_item: row.id_response_item})
#                             MERGE (source)-[r:IS_ITEM]->(target)
#                             RETURN COUNT(r) AS total
#                         """
#                 return insert_data(query, rows)
#             add_responses_response_pattern_items_rel(adjacency_input_form_display_df)

#         return adjacency_input_form_display_df

#     else:

#         return None

# a = [FD_insert_responses(response_tuple, {1,2,3}) \
#      for response_tuple in adjacency_input_form_list]

close_all_sessions()

with session_scope() as session:
    queried_responses_complete = (
        session.query(IRA_Adjacency_input_form, IRA_Responses).filter(
            IRA_Adjacency_input_form.id_adjacency_input_form ==
            IRA_Responses.id_adjacency_input_form).all())
    queried_questions = session.query(IRA_Questions).all()

queried_responses_123_complete_list = [
    (r.id_response, r.Response, r.id_question, r.id_adjacency_input_form,
        aif.id_employee ) for aif, r in queried_responses_complete
            if r.id_question != 4]

queried_responses_123_complete_df = pd.DataFrame(
    queried_responses_123_complete_list,
    columns=['id_response', 'response', 'id_question', 'id_adjacency_input_form',
                'id_employee'])

queried_responses_123_complete_df['id_interacting_employee'] = (
    queried_responses_123_complete_df.apply(
        lambda x: json.loads(x['response'])[0]['valor'], axis=1))

queried_responses_123_complete_df = (
    queried_responses_123_complete_df.explode('id_interacting_employee'))

queried_responses_123_complete_df['id_interacting_employee'] =(
    queried_responses_123_complete_df.apply(
        lambda x: int(x['id_interacting_employee']), axis=1))


queried_responses_4_complete_list = [
    (r.id_response, r.Response, r.id_question, r.id_adjacency_input_form,
        aif.id_employee ) for aif, r in queried_responses_complete
            if r.id_question == 4]
queried_responses_4_complete_list

questions_list = [(q.id_question, q.id_question_possible_answers)
                    for q in queried_questions]
questions_list

questions_df = pd.DataFrame(
    questions_list, columns=['id_question', 'id_question_possible_answers'])

queried_responses_4_df = pd.DataFrame(
    queried_responses_4_complete_list,
    columns=['id_response', 'response', 'id_question', 'id_adjacency_input_form',
                'id_employee'])

queried_responses_4_complete_df = pd.merge(
    queried_responses_4_df, questions_df, left_on='id_question',
    right_on='id_question',how = 'left')

def valores_to_list(xresponse):
    """sumary_line"""
    valores_dict = json.loads(xresponse)[0]['valor']
    valores_list = [(k,v) for k,v in valores_dict.items()]
    return valores_list


queried_responses_4_complete_df['valor_dictionary_tuple'] =(
    queried_responses_4_complete_df.apply(
        lambda x: valores_to_list(x['response']), axis=1))
list(queried_responses_4_complete_df['valor_dictionary_tuple'])

queried_responses_4_complete_df.shape

queried_responses_4_complete_df = (
    queried_responses_4_complete_df.explode('valor_dictionary_tuple'))
queried_responses_4_complete_df.shape

list(queried_responses_4_complete_df['valor_dictionary_tuple'])
queried_responses_4_complete_df.shape

queried_responses_4_complete_df.dropna(subset=['valor_dictionary_tuple'],
                                        inplace=True)
queried_responses_4_complete_df.shape

def interacting_valores_to_list(xresponse):
    """sumary_line"""
    # print('xresponse', xresponse)
    interacting_employee = xresponse[0]
    valores = xresponse[1]['options']
    text = xresponse[1]['text']
    return pd.Series([interacting_employee, valores, text])

queried_responses_4_complete_df[['id_interacting_employee','valor','text']] = (
    queried_responses_4_complete_df.apply(
        lambda x: interacting_valores_to_list(x['valor_dictionary_tuple']), axis=1))
queried_responses_4_complete_df['id_interacting_employee']
queried_responses_4_complete_df['valor']
list(queried_responses_4_complete_df['text'])

queried_responses_4_complete_df.shape

queried_responses_4_complete_df = queried_responses_4_complete_df.explode('valor')
queried_responses_4_complete_df.shape

queried_responses_4_complete_df.fillna({'valor': -1}, inplace=True)

queried_responses_4_complete_df.reset_index(drop=True, inplace=True)
queried_responses_4_complete_df.columns
queried_responses_4_complete_df.reset_index(inplace=True)

queried_responses_4_complete_df['id_response_pattern_item'] =(
    queried_responses_4_complete_df.apply(
        lambda x: '3-'+str(x['valor']) if x['valor'] != -1 else '3-'+str(x['index']),
        axis=1))
queried_responses_4_complete_df['id_response_pattern_item']
queried_responses_4_complete_df.shape


queried_responses_4_complete_df.loc[queried_responses_4_complete_df.text != '']


queried_responses_4_complete_df['id_interacting_employee'] =(
    queried_responses_4_complete_df.apply(
        lambda x: int(x['id_interacting_employee']), axis=1))

queried_responses_4_complete_df['comodín'] = 'comodín'

queried_responses_4_complete_df.drop_duplicates(
    ['id_question','id_response'])[['id_question','id_response']]
queried_responses_4_complete_df.drop_duplicates(
    ['id_response','id_interacting_employee'])[['id_response','id_interacting_employee']]
queried_responses_4_complete_df.drop_duplicates(
    ['id_response','id_response_pattern_item'])[['id_response','id_response_pattern_item']]
queried_responses_4_complete_df.drop_duplicates(
    ['index','id_response_pattern_item'])[['index','id_response_pattern_item']]

def add_response(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MERGE (:Response {id_response: row.id_response})
                RETURN COUNT(*) AS total
            """
    return insert_data(query, rows)
add_response(queried_responses_123_complete_df)

add_response(queried_responses_4_complete_df)

def add_responses_questions_rel(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MATCH (source:Response {id_response: row.id_response})
                MATCH (target:Question {id_question: row.id_question})
                MERGE (source)-[r:FOR_QUESTION]->(target)
                RETURN COUNT(r) AS total
            """
    return insert_data(query, rows)
add_responses_questions_rel(queried_responses_123_complete_df)

add_responses_questions_rel(queried_responses_4_complete_df)

def add_responses_adjacency_input_forms_rel(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MATCH (source: Response {id_response: row.id_response})
                MATCH (target: Adjacency_input_form {id_adjacency_input_form:
                                                row.id_adjacency_input_form})
                MERGE (source)-[r:OF_FORM]->(target)
                RETURN COUNT(r) AS total
            """
    return insert_data(query, rows)
add_responses_adjacency_input_forms_rel(queried_responses_123_complete_df)

add_responses_adjacency_input_forms_rel(queried_responses_4_complete_df)

# if id_network_mode in xactor_network_modes:
def add_responses_related_employees_rel(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MATCH (source: Response {id_response: row.id_response})
                MATCH (target: Employee {id_employee: row.id_interacting_employee})
                MERGE (source)-[r:RELATED_TO]->(target)
                RETURN COUNT(r) AS total
            """
    return insert_data(query, rows)
add_responses_related_employees_rel(queried_responses_123_complete_df)

add_responses_related_employees_rel(queried_responses_4_complete_df)

def add_employees_responses_rel(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MATCH (source: Employee {id_employee: row.id_employee})
                MATCH (target: Response {id_response: row.id_response})
                MERGE (source)-[r:HAS_RESPONSE]->(target)
                RETURN COUNT(r) AS total
            """
    return insert_data(query, rows)
add_employees_responses_rel(queried_responses_123_complete_df)

add_employees_responses_rel(queried_responses_4_complete_df)

def add_response_pattern_items(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MERGE (e:Response_pattern_item
                        {id_response_pattern_item: row.id_response_pattern_item,
                            item_meaning: row.comodín,
                            item_value: row.valor})
                RETURN COUNT(*) AS total
            """
    return insert_data(query, rows)
add_response_pattern_items(
    queried_responses_4_complete_df.loc[queried_responses_4_complete_df.text != ''])
queried_responses_4_complete_df.loc[queried_responses_4_complete_df.text != '']

def add_responses_response_pattern_items_rel(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MATCH (source: Response {id_response: row.id_response})
                MATCH (target: Response_pattern_item
                        {id_response_pattern_item: row.id_response_pattern_item})
                MERGE (source)-[r:IS_ITEM]->(target)
                RETURN COUNT(r) AS total
            """
    return insert_data(query, rows)
add_responses_response_pattern_items_rel(queried_responses_4_complete_df)

def add_responses_texts(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MERGE (e:Response_text {id_response_text: row.index,
                                        id_response: row.id_response,
                                        response_text: row.text})
                RETURN COUNT(*) AS total
            """
    return insert_data(query, rows)
add_responses_texts(
    queried_responses_4_complete_df.loc[queried_responses_4_complete_df.text != ''])
queried_responses_4_complete_df.loc[queried_responses_4_complete_df.text != '']

def add_response_pattern_items_responses_texts_rel(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MATCH (source: Response_pattern_item
                        {id_response_pattern_item: row.id_response_pattern_item})
                MATCH (target: Response_text {id_response_text: row.index})
                MERGE (source)-[r:HAS_TEXT]->(target)
                RETURN COUNT(r) AS total
            """
    return insert_data(query, rows)
add_response_pattern_items_responses_texts_rel(
    queried_responses_4_complete_df.loc[queried_responses_4_complete_df.text != ''])

queried_responses_4_complete_df.loc[queried_responses_4_complete_df.text != '']

queried_responses_4_complete_df.drop_duplicates(
    ['index','id_response_pattern_item'])[['index','id_response_pattern_item']]

def add_target_employees_responses_texts_rel(rows):
    """sumary_line"""
    query = """UNWIND $rows AS row
                MATCH (source: Response_text {id_response_text: row.index})
                MATCH (target: Employee {id_employee: row.id_interacting_employee})
                MERGE (source)-[r:TEXT_ABOUT]->(target)
                RETURN COUNT(r) AS total
            """
    return insert_data(query, rows)
add_target_employees_responses_texts_rel(queried_responses_4_complete_df)

queried_responses_4_complete_df.columns




