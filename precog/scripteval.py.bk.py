
import logging
import logging.config

import ServerSideExtension_pb2 as SSE
import grpc
import numpy
import pandas
from ssedata import ArgType, FunctionType, ReturnType


class ScriptEval:
    """
    Class for SSE plugin ScriptEval functionality.
    """

    def EvaluateScript(self, header, request, context):
        """
        Evaluates script provided in the header, given the
        arguments provided in the sequence of RowData objects, the request.
        :param header:
        :param request: an iterable sequence of RowData.
        :param context: the context sent from client
        :return: an iterable sequence of RowData.
        """
        # Retrieve function type
        func_type = self.get_func_type(header)
        logging.debug("Function Type {}" .format(func_type))
        # Retrieve data types from header
        arg_types = self.get_arg_types(header)
        logging.debug("Argument Type {}" .format(arg_types))
        ret_type = self.get_return_type(header)
        logging.debug("Return Type {}" .format(ret_type))

        logging.info('EvaluateScript: {} ({} {}) {}'
                     .format(header.script, arg_types, ret_type, func_type))

        # Create a panda data frame, for retrieved parameters
        q = pandas.DataFrame()

        # Check if parameters are provided
        if header.params:
            # Iterate over bundled rows
            logging.debug(header.params)
            for request_rows in request:
                # Iterate over rows
                for row in request_rows.rows:
                    # Retrieve parameters and append to data frame
                    logging.debug(request_rows.rows)
                    params, dual_exist = self.get_arguments(context, arg_types, row.duals, header)
                    q = q.append(params, ignore_index=True)
                    logging.debug('Printing Dual, Param, q {}, {} {}' .format(row.duals, params, q))
            # Rename columns based on arg names in header
            arg_names = [param.name for param in header.params]
            if dual_exist:
                # find what column(s) are dual
                param_types = [param.dataType for param in header.params]
                col_index = [i for i, arg_type in enumerate(param_types) if arg_type == SSE.DUAL]
                # add _num and _str columns representing the dual column
                # for an easier access in the script
                for col in col_index:
                    arg_names.insert(col + 1, arg_names[col] + '_str')
                    arg_names.insert(col + 2, arg_names[col] + '_num')
            q.rename(columns=lambda i: arg_names[i], inplace=True)
            logging.debug('Printing Yield + Col {}, {}, {}, {}' .format(context, header.script, ret_type, q))
            yield self.evaluate(context, header.script, ret_type, q)

        else:
            # No parameters provided
            logging.debug('No Parameter Provided')
            yield self.evaluate(context, header.script, ret_type, q)

    @staticmethod
    def get_func_type(header):
        """
        Retrieves the function type.
        :param header:
        :return:
        """
        func_type = header.functionType
        if func_type == SSE.SCALAR:
            return FunctionType.Scalar
        elif func_type == SSE.AGGREGATION:
            return FunctionType.Aggregation
        elif func_type == SSE.TENSOR:
            return FunctionType.Tensor

    @staticmethod
    def raise_grpc_error(context, status_code, msg):
        # Make sure the error handling, including logging, works as intended in the client
        context.set_code(status_code)
        context.set_details(msg)
        # Raise error on the plugin-side
        raise grpc.RpcError(status_code, msg)

    def get_arguments(self, context, arg_types, duals, header):
        """
        Gets the array of arguments based on
        the duals, and the type (string, numeric)
        specified in the header.
        :param context: the context sent from client
        :param arg_types: the argument data type
        :param duals: an iterable sequence of duals.
        :param header: the script header.
        :return: a panda Series containing (potentially mixed data type) arguments.
        """
        dual_type = False
        if arg_types == ArgType.String:
            # All parameters are of string type
            script_args = [d.strData for d in duals]
        elif arg_types == ArgType.Numeric:
            # All parameters are of numeric type
            script_args = [d.numData for d in duals]
        elif arg_types == ArgType.Mixed:
            # Parameters can be either string, numeric or dual
            script_args = []
            for dual, param in zip(duals, header.params):
                if param.dataType == SSE.STRING:
                    script_args.append(dual.strData)
                elif param.dataType == SSE.NUMERIC:
                    script_args.append(dual.numData)
                elif param.dataType == SSE.DUAL:
                    script_args.append((dual.numData, dual.strData))
                    # We add additional columns with string and numeric representation
                    # for easier access in script
                    script_args.append(dual.strData)
                    script_args.append(dual.numData)
                    dual_type = True
        else:
            # Undefined argument types
            msg = 'Undefined argument type: '.format(arg_types)
            self.raise_grpc_error(context, grpc.StatusCode.INVALID_ARGUMENT, msg)

        # dtype=object is needed if the data is not homogeneous, e.g. data type dual
        return pandas.Series(script_args, dtype=object), dual_type

    @staticmethod
    def get_arg_types(header):
        """
        Determines the argument types for all parameters.
        :param header:
        :return: ArgType
        """
        data_types = [param.dataType for param in header.params]
        logging.debug("Datatypes {}" .format(data_types))
        if not data_types:
            return ArgType.Empty
        elif len(set(data_types)) > 1 or all(data_type == SSE.DUAL for data_type in data_types):
            return ArgType.Mixed
        elif all(data_type == SSE.STRING for data_type in data_types):
            return ArgType.String
        elif all(data_type == SSE.NUMERIC for data_type in data_types):
            return ArgType.Numeric
        else:
            return ArgType.Undefined
        

    @staticmethod
    def get_return_type(header):
        """
        :param header:
        :return: Return type
        """
        if header.returnType == SSE.STRING:
            return ReturnType.String
        elif header.returnType == SSE.NUMERIC:
            return ReturnType.Numeric
        elif header.returnType == SSE.DUAL:
            return ReturnType.Dual
        else:
            return ReturnType.Undefined

    @staticmethod
    def get_duals(result, ret_type):
        """
        Transforms one row in qResult to an iterable of duals.
        :param result: one row of qResult
        :param ret_type: a list containing a data type for each column in qResult
        :return: one row of data as an iterable of duals
        """
        # result must be iterable
        result = [result] if isinstance(result, (str, tuple)) or not hasattr(result, '__iter__') else result
        # Transform the result to an iterable of Dual data
        duals = []
        for i, col in enumerate(result):
            data_type = ret_type[i]
            if data_type == ReturnType.String:
                duals.append(SSE.Dual(strData=col))
            elif data_type == ReturnType.Numeric:
                duals.append(SSE.Dual(numData=col))
            elif data_type == ReturnType.Dual:
                # col is a tuple with a numeric and string representation
                duals.append(SSE.Dual(numData=col[0], strData=col[1]))
        return iter(duals)

    @staticmethod
    def send_table_description(table, context):
        """
        # TableDescription is only handled in Qlik if sent from a 'Load ... Extension ...' script.
        # If tableDescription is set when evaluating an expression the header will be ignored
        # when received by Qlik.
        :param qResult: the result from evaluating the script
        :param table: the table description specified in the script
        :param context: the request context
        :return: nothing
        """
        logging.debug('tableDescription sent to Qlik: {}'.format(table))
        # send table description
        table_header = (('qlik-tabledescription-bin', table.SerializeToString()),)
        context.send_initial_metadata(table_header)



