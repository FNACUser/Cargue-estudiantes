# -*- coding: utf-8 -*-
"""
Created on Wed Jun 28 19:08:23 2023

@author: luis.caro
"""

import pandas as pd
from ast import literal_eval
import json

# from UtilitiesBokeh import UTBo_DataFrame_to_DataTable


def zip_expand(xindex_list, xvalues_list):
    # print('.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-. zip_expand')
    zipped_lists = zip(xindex_list, xvalues_list)
    zipped_expanded_list = [(i, v) for i, vlist in zipped_lists for v in vlist]
    # print('>>>>>>>>>>>>>>>>>> zipped_expanded_list (zip_expand)')
    # print(zipped_expanded_list)
    return zipped_expanded_list 


# def FD_parse_response(xid_question, xresponse,
#                         xid_question_possible_answers,
#                         xquestions_possible_answers_dict,
#                         xquestions_multiple_option_indicator_dict,
#                         xid_employee, xemployee, xemployees_df):
    
#     # print('.-.-.-.-.-.-.-.-.-.-.-.-.-.-.- FD_process_response')
    
#     #
#     #question_possible_answers_dict: diccionario de posibles respuestas
#     #                                para la pregutna que se está procesando
#     #   - llave: id_respuesta
#     #   - valor: texto respuesta
#     question_possible_answers_dict = \
#         xquestions_possible_answers_dict.get(xid_question_possible_answers)
#     # print('>>>>>>>>>> question_possible_answers_dict (FD_process_response)')
#     # print(question_possible_answers_dict)
    
    
#     # response_json = [response.Response for response in responses 
#     #             if response.id_question == xquestion.id_question]
    
#     response_json = [xresponse]
    
#     if len(response_json) > 0:
#         valores_str = get_values_from_dict(response_json[0],'valor')
#         valores = [valor_str for valor_str in valores_str]
        
#         ids_nodos_str = \
#             get_values_from_dict(response_json[0],'item_id')
#         ids_nodos = [int(id_nodo_str) for id_nodo_str in ids_nodos_str]
        
#         if xquestions_multiple_option_indicator_dict.get(xid_question_possible_answers):
#             eval_valores = [literal_eval(valor) for valor in valores]
#             node_value_tuples_list_nonstr = zip_expand(ids_nodos, eval_valores)
#             node_value_tuples_list = \
#                 [(nodo, str(valor)) \
#                  for nodo, valor in node_value_tuples_list_nonstr]
#         else:
#             node_value_tuples_list = list(zip(ids_nodos, valores))        
        
#         questions_responses_list = \
#             [(node, question_possible_answers_dict.get(value)) \
#               for node, value in node_value_tuples_list]
#         questions_responses_df = \
#             pd.DataFrame(questions_responses_list, 
#                          columns=['id_interacting_employee','response'])
#         questions_responses_df['id_employee'] = xid_employee
#         questions_responses_df['employee'] = xemployee
#         questions_responses_df['id_question'] = xid_question
        
#         def interacting_employee(xrow):
            
#             interacting_employee =\
#                 list(xemployees_df.loc[xemployees_df.id_employee\
#                                        == xrow['id_interacting_employee']]\
#                                            ['employee'])[0]
#             return interacting_employee
        
#         questions_responses_df['interacting_employee'] =\
#             questions_responses_df.apply(interacting_employee, axis=1)
#     else:
#         questions_responses_df = \
#             pd.DataFrame(columns=['id_interacting_employee','response',
#                                   'id_employee', 'employee', 'id_question',
#                                   'interacting_employee'])
            
#     questions_responses_df = \
#         questions_responses_df[['id_question', 'id_employee', 'employee', 
#                                 'id_interacting_employee',
#                                 'interacting_employee', 'response']]
        
#     return questions_responses_df


# def FD_process_response(xaif_responses_json_df,
#                         xquestions_ids_possible_answers_dict,
#                         xquestions_possible_answers_dict,
#                         xquestions_multiple_option_indicator_dict,
#                         xid_employee, xemployee, xemployees_df):
    
#     adjacency_input_form_display_df = \
#         pd.DataFrame(columns=['id_question', 'id_employee', 'employee', 
#                               'id_interacting_employee', 
#                               'interacting_employee', 'response'])
                             
#     #
#     #para cada una de las respuestas de las preguntas se ejecuta el 
#     #siguiente proceso (las respuestas son un vector de respuestas
#     #por pregunta):
#     for question_number in range(xaif_responses_json_df.shape[0]):
#         _id_question = \
#             list(xaif_responses_json_df['id_question'])[question_number]
#         _response = \
#             list(xaif_responses_json_df['response'])[question_number]
#         _id_possible_responses = \
#             xquestions_ids_possible_answers_dict.get(_id_question)
        
#         aif_question_responses_df = \
#             FD_parse_response\
#                 (_id_question, _response, _id_possible_responses,
#                  xquestions_possible_answers_dict,
#                  xquestions_multiple_option_indicator_dict,
#                  xid_employee, xemployee, xemployees_df)
        
#         adjacency_input_form_display_df = \
#             adjacency_input_form_display_df.append\
#                 (aif_question_responses_df, ignore_index=True)
                
#     return adjacency_input_form_display_df

#import from funciones_leer_json_parrafos
def get_values_from_dict(xrtas_pregunta,xfield):
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


def FD_parse_response(xid_question, xresponse,
                        xid_question_possible_answers,
                        xquestions_possible_answers_patterns_dict,
                        xquestions_multiple_option_indicator_dict,
                        xid_employee):
    
    print('.-.-.-.-.-.-.-.-.-.-.-.-.-.-.- FD_parse_response')
    print('>>>>>>>>>>> xid_question_possible_answers (FD_parse_response)')
    print(xid_question_possible_answers)
    print('>>>>>>>> xquestions_possible_answers_patterns_dict (FD_parse_response)')
    print(xquestions_possible_answers_patterns_dict)

    #
    #question_possible_answers_dict: diccionario de posibles respuestas
    #                                para la pregutna que se está procesando
    #   - llave: id_respuesta
    #   - valor: texto respuesta
    question_possible_answers_dict = \
        xquestions_possible_answers_patterns_dict.get\
            (xid_question_possible_answers)
    print('>>>>>>>>>> question_possible_answers_dict (FD_parse_response)')
    print(question_possible_answers_dict)
    
    
    # response_json = [response.Response for response in responses 
    #             if response.id_question == xquestion.id_question]
    
    response_json = [xresponse]
    print('>>>>>>>>>> response_json (FD_parse_response)')
    print(response_json)
    
    if len(response_json) > 0:
        valores_str = get_values_from_dict(response_json[0],'valor')
        valores = [valor_str for valor_str in valores_str]
        print('>>>>>>>>>> valores (FD_parse_response)')
        print(valores)
        
        
        ids_nodos_str = \
            get_values_from_dict(response_json[0],'item_id')
        ids_nodos = [int(id_nodo_str) for id_nodo_str in ids_nodos_str]
        
        if xquestions_multiple_option_indicator_dict.get(xid_question_possible_answers):
            eval_valores = [literal_eval(valor) for valor in valores]
            node_value_tuples_list_nonstr = zip_expand(ids_nodos, eval_valores)
            node_value_tuples_list = \
                [(nodo, str(valor)) \
                 for nodo, valor in node_value_tuples_list_nonstr]
        else:
            node_value_tuples_list = list(zip(ids_nodos, valores))        
        
        questions_responses_list = \
            [(node, str(xid_question_possible_answers),
              str(xid_question_possible_answers)+'-'+value, 
              question_possible_answers_dict.get(value)) \
              for node, value in node_value_tuples_list]
        # zquestion_possible_answers_dict.get(value)) \
        
        print('>>>>>>>>>>> questions_responses_list (FD_parse_response)')
        print(questions_responses_list)
        
        if len(questions_responses_list) > 0:
            questions_responses_df = \
                pd.DataFrame(questions_responses_list, 
                             columns=['id_interacting_employee', 
                                      'id_response_pattern', 
                                      'id_response_item',
                                      'response'])
            questions_responses_df['id_employee'] = xid_employee
            # questions_responses_df['employee'] = xemployee
            questions_responses_df['id_question'] = xid_question
        else:
            questions_responses_df = \
                pd.DataFrame(columns=['id_interacting_employee', 
                                      'id_response_pattern', 
                                      'id_response_item', 
                                      'response',
                                      'id_employee', 'id_question'])
        
        # def interacting_employee(xrow):
            
        #     interacting_employee =\
        #         list(xemployees_df.loc[xemployees_df.id_employee\
        #                                == xrow['id_interacting_employee']]\
        #                                    ['employee'])[0]
        #     return interacting_employee
        
        # questions_responses_df['interacting_employee'] =\
        #     questions_responses_df.apply(interacting_employee, axis=1)
    else:
        questions_responses_df = \
            pd.DataFrame(columns=['id_interacting_employee', 
                                  'id_response_pattern', 
                                  'id_response_item', 
                                  'response',
                                  'id_employee', 'id_question'])
            
    # questions_responses_df = \
    #     questions_responses_df[['id_question', 'id_employee', 
    #                             'id_interacting_employee',
    #                             'response']]
        
    print('>>>>>>>>>>> questions_responses_df (FD_parse_response)')
    print(questions_responses_df.columns)
    print(questions_responses_df.shape)
    print(questions_responses_df.to_dict('records'))
    
    return questions_responses_df


def FD_process_response(xaif_responses_json_df,
                        xquestions_ids_possible_answers_patterns_dict,
                        xquestions_possible_answers_patterns_dict,
                        xquestions_multiple_option_indicator_dict,
                        xid_employee):
    
    print('.-.-.-.-.-.-.-.-.-.-.-.-.-.-.- FD_process_response')
    print('xaif_responses_json_df FD_process_response')
    print(xaif_responses_json_df.shape)
    print(xaif_responses_json_df.shape)
    print('>>>>> xquestions_ids_possible_answers_patterns_dict (FD_process_response)')
    print(xquestions_ids_possible_answers_patterns_dict)
    print('>>>>> xquestions_possible_answers_patterns_dict (FD_process_response)')
    print(xquestions_possible_answers_patterns_dict)

    adjacency_input_form_display_df = \
        pd.DataFrame(columns=['id_question', 'id_employee', 
                              'id_interacting_employee', 
                              'id_response_pattern',
                              'id_response_item', 'response'])
                         
    #
    #para cada una de las respuestas de las preguntas se ejecuta el 
    #siguiente proceso (las respuestas son un vector de respuestas
    #por pregunta):
    for question_number in range(xaif_responses_json_df.shape[0]):
        _id_question = \
            list(xaif_responses_json_df['id_question'])[question_number]
        _response = \
            list(xaif_responses_json_df['response'])[question_number]
        _id_possible_responses = \
            xquestions_ids_possible_answers_patterns_dict.get(_id_question)
        
        aif_question_responses_df = \
            FD_parse_response\
                (_id_question, _response, _id_possible_responses,
                 xquestions_possible_answers_patterns_dict,
                 xquestions_multiple_option_indicator_dict,
                 xid_employee)
        
        # adjacency_input_form_display_df = \
        #     adjacency_input_form_display_df.append\
        #         (aif_question_responses_df, ignore_index=True)
        adjacency_input_form_display_df = \
            pd.concat([adjacency_input_form_display_df,
                       aif_question_responses_df], ignore_index=True)
                
    return adjacency_input_form_display_df


def FD_employee_interactions(xid_employee, xemployees_df, 
                             xemployees_interactions_df):
    
    print('.-.-.-.-.-.-.-.-.-.-.-.-.-.- FD_employee_interactions')
    
    #busca los funcionarios que interactúan con quien responde
    interacting_employees_ids_df = \
        xemployees_interactions_df.\
            loc[xemployees_interactions_df['id_responding_employee'] ==\
                xid_employee]
                                                      
    #integra los datos de los funcionarios desde employees_df
    interacting_employees_df = \
        pd.merge(interacting_employees_ids_df, xemployees_df,
                 left_on = 'id_interacting_employee', 
                 right_on = 'id_employee', how = 'left')\
            [['id_interacting_employee', 'employee']]
    interacting_employees_df['id_employee'] = xid_employee
    interacting_employees_df.\
        rename(columns={'employee':'interacting_employee'}, inplace=True)
    
    print('>>>>>>>>>>>>> interacting_employees_df (FD_employee_interactions)')
    print(interacting_employees_df.to_dict('records'))
    
    return interacting_employees_df


def FD_select_questions(xid_network_mode, xquestions_df, 
                        xquestions_vs_networks_modes_df):
    
    # print('.-.-.-.-.-.-.-.-.-.-.-.-.-. FD_select_questions')
    
    selected_questions_vs_networks_modes_df =\
        xquestions_vs_networks_modes_df.\
            loc[xquestions_vs_networks_modes_df.id_network_mode ==\
                xid_network_mode]
    
    network_mode_questions_df = \
        pd.merge(selected_questions_vs_networks_modes_df,
                 xquestions_df, left_on = 'id_question',
                 right_on = 'id_question', how = 'left')\
            [['id_question', 'question', 'id_question_possible_answers']]
    
    return network_mode_questions_df


def function_source(xid_employee,
                    xid_network_mode,
                    xid_adjacency_input_form,
                    xquestions_df,
                    xquestions_vs_networks_modes_df, xresponses_df,
                    xemployees_df, xemployees_interactions_df,
                    xquestions_possible_answers_patterns_dict,
                    xquestions_multiple_option_indicator_dict):
    
    print('.-..-..-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.- function_source')
    print('>>>>> xquestions_possible_answers_patterns_dict (function_source)')
    print(xquestions_possible_answers_patterns_dict)
    print('>>>>> xid_employee (function_source)')
    print(xid_employee)
    print('>>>>> xid_network_mode (function_source)')
    print(xid_network_mode)
    print('>>>>> xid_adjacency_input_form (function_source)')
    print(xid_adjacency_input_form)
    print('>>>>> list(xemployees_df[id_employee]) (function_source)')
    print(list(xemployees_df['id_employee']))
    
    #
    #network_mode_questions_df son las preguntas que corresponden al
    #adajcency_input_form:
    network_mode_questions_df = \
        FD_select_questions(xid_network_mode, xquestions_df,
                            xquestions_vs_networks_modes_df)
    print('>>>>> network_mode_questions_df (function_source)')
    print(network_mode_questions_df.shape)
    print(network_mode_questions_df)
    
    #
    #questions_ids_possible_answers_dict es un dicionario:
    #   - llave: id_question (una llave para cada pregunta del 
    #            adjacency_input_form)
    #   - valor: LLave de las possibles respuestas en 
    #            xquestions_possible_answers_dict
    questions_ids_possible_answers_patterns_dict =\
        {k:v for k,v in zip(list(network_mode_questions_df['id_question']),
                        list(network_mode_questions_df\
                             ['id_question_possible_answers']))}
    print('>>>>>>>>>>>> questions_ids_possible_answers_dict (function_source)')
    print(questions_ids_possible_answers_patterns_dict)
    
    #
    #aif_responses_jason_df: respuestas a las pregunttas
    aif_responses_json_df = \
        xresponses_df.loc[xresponses_df.id_adjacency_input_form ==\
                          xid_adjacency_input_form]\
            [['id_adjacency_input_form', 'id_question', 'response']]
    print('>>>>>>>>>>>> aif_responses_json_df (function_source)')
    print(aif_responses_json_df.shape)
    print(aif_responses_json_df)
    
    employee_record = xemployees_df.loc[xemployees_df['id_employee'] ==\
                                        xid_employee]
    print('>>>>>>>>>>>> employee_record (function_source)')
    print(employee_record.shape)
    print(employee_record)
    
    employee = list(employee_record['employee'])[0]
    
    # adjacency_input_form_display_df = \
    #     pd.DataFrame(columns=['id_question', 'id_employee', 'employee', 
    #                           'id_interacting_employee', 
    #                           'interacting_employee', 'response'])
    
    
    #
    #se inicializa adjacency_input_form_display_df con una fila
    #por funcionaro con el que se interactúa en el adjacency_input_form,
    #con las siguine
    
    # if xnetwork_name == 'Actor':
    #     interacting_employees_df = \
    #         FD_employee_interactions(xid_employee, xemployees_df,
    #                                   xemployees_interactions_df)
    # else:
    #     interacting_employees_df = \
    #         FD_employee_interactions(xid_employee, xemployees_df,
    #                                  xemployees_interactions_df)
        
        # print('>>>>>>> adjacency_input_form_display_df (function_source)')
        # print(adjacency_input_form_display_df)
        
    #
    #para cada una de las respuestas de las preguntas se ejecuta el 
    #siguiente proceso (las respuestas son un vector de respuestas
    #por pregunta):
    # for question_number in range(aif_responses_json_df.shape[0]):
    #     _id_question = \
    #         list(aif_responses_json_df['id_question'])[question_number]
    #     _response = \
    #         list(aif_responses_json_df['response'])[question_number]
    #     _id_possible_responses = \
    #         questions_ids_possible_answers_dict.get(_id_question)
        
    #     aif_question_responses_df = \
    #         FD_process_response\
    #             (_id_question, _response, _id_possible_responses,
    #              xquestions_possible_answers_dict,
    #              xquestions_multiple_option_indicator_dict,
    #              id_employee, employee, xemployees_df)
        
    #     adjacency_input_form_display_df = \
    #         adjacency_input_form_display_df.append\
    #             (aif_question_responses_df, ignore_index=True)
    adjacency_input_form_display_df = \
        FD_process_response(aif_responses_json_df,
                            questions_ids_possible_answers_patterns_dict,
                            xquestions_possible_answers_patterns_dict,
                            xquestions_multiple_option_indicator_dict,
                            xid_employee)
    
    print('>>>>>> adjacency_input_form_display_df.columns 1 (function_source)')
    print(adjacency_input_form_display_df.columns)
    print(adjacency_input_form_display_df.shape) 
    
    
    def get_interacting_employee(xrow):
        
        print('.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-. get_interacting employee')
        print('>>>>>>>>>>>>>>>>>> xrow (get_interacting employee)')
        print(xrow)
        
        interacting_employee_list =\
            list(xemployees_df.loc[xemployees_df.id_employee\
                                    == xrow['id_interacting_employee']]\
                                        ['employee'])
        if len(interacting_employee_list) > 0:
            interacting_employee = interacting_employee_list[0]
        else:
            interacting_employee = 'NA'
        print('>>>>>>>>>>>>> interacting_employee (get_interacting employee)')
        print(interacting_employee)
        
        return interacting_employee
    
    def create_id_response(xrow):
        id_response = xrow['id_adjacency_input_form'] + '_' +\
            str(xrow['id_question']) + '_' +\
                str(xrow['id_interacting_employee']) + '_' +\
                    xrow['id_response_item']
        return id_response
    
    if adjacency_input_form_display_df.shape[0] > 0:
        
        adjacency_input_form_display_df['employee'] = employee
            
        adjacency_input_form_display_df['interacting_employee'] =\
            adjacency_input_form_display_df.apply(get_interacting_employee, 
                                                  axis=1)
        adjacency_input_form_display_df['id_adjacency_input_form'] =\
            xid_adjacency_input_form
        adjacency_input_form_display_df['id_response'] =\
            adjacency_input_form_display_df.apply(create_id_response, axis=1)
                
    else:
        adjacency_input_form_display_df = \
            pd.DataFrame(columns=['id_question', 'id_employee', 
                                  'id_interacting_employee', 
                                  'id_response_pattern',
                                  'id_response_item', 'response',
                                  'employee', 'interacting_employee',
                                  'id_adjacency_input_form',
                                  'id_response'])
        
        
        
    print('>>>>>> adjacency_input_form_display_df.columns 2 (function_source)')
    print(adjacency_input_form_display_df.to_dict('records'))
    print(adjacency_input_form_display_df.columns)
    print(adjacency_input_form_display_df.shape) 
    
    adjacency_input_form_display_df = \
        adjacency_input_form_display_df[['id_question', 'id_employee', 
                                         'employee', 'id_interacting_employee',
                                         'interacting_employee',
                                         'id_response_pattern',
                                         'id_response_item', 'response',
                                         'id_adjacency_input_form',
                                         'id_response']] 
        
    
    print('>>>>>> adjacency_input_form_display_df.columns 3 (function_source)')
    print(adjacency_input_form_display_df.columns)
    print(adjacency_input_form_display_df.shape) 
    
    return network_mode_questions_df, aif_responses_json_df, \
        adjacency_input_form_display_df

#questions_ids_possible_answers_patterns_dict,\

    
def FD_Questions_possible_answers(xquestions_possible_answers):
    
    def zip_dicts(xk,xv):
        values = dict_valores.get(xk)
        dicts_dict = {text:value for text, value in list(zip(values, xv))}
        return dicts_dict
    
    def values_from_json(xfield):
        dict_values = \
            {question_possible_answers.id_question_possible_answers:\
             get_values_from_dict\
                 (question_possible_answers.Question_possible_answers_es, 
                  xfield) \
         for question_possible_answers in xquestions_possible_answers \
             if question_possible_answers.Question_possible_answers_es != ''}
                
        return dict_values
        
    # with session_scope() as session:
    #     questions_possible_answers = \
    #         session.query(IRA_Questions_possible_answers).all()

    dict_textos = values_from_json('texto')
    dict_valores = values_from_json('valor')
    
    questions_possible_answers_patterns_dict = \
        {k:zip_dicts(k,t) for k,t in dict_textos.items()}
        
    questions_multiple_option_indicator_dict = \
        {qpe.id_question_possible_answers:qpe.multiple \
         for qpe in xquestions_possible_answers}
    
    
    return questions_possible_answers_patterns_dict, \
        questions_multiple_option_indicator_dict


