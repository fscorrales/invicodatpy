from dataclasses import dataclass
from typing import cast

from pandas import DataFrame
from pandas._config import get_option
from pandas.io.formats import format as fmt
from pandas.io.formats.format import DataFrameFormatter, format_array

class TidyDataFrameFormatter(DataFrameFormatter):
    def _truncate_horizontally(self) -> None:
        """Remove columns, which are not to be displayed and adjust formatters.
        Attributes affected:
            - tr_frame
            - formatters
            - tr_col_num
        """
        assert self.max_cols_fitted is not None
        # col_num = self.max_cols_fitted // 2
        col_num = self.max_cols_fitted
        if col_num >= 1:
            # left = self.tr_frame.iloc[:, :col_num]
            # right = self.tr_frame.iloc[:, -col_num:]
            # self.tr_frame = concat((left, right), axis=1)
            self.tr_frame = self.tr_frame.iloc[:, :col_num]

            # truncate formatter
            if isinstance(self.formatters, (list, tuple)):
                # self.formatters = [
                #     *self.formatters[:col_num],
                #     *self.formatters[-col_num:],
                # ]
                self.formatters = self.formatters[:col_num]
                    
        else:
            col_num = cast(int, self.max_cols)
            self.tr_frame = self.tr_frame.iloc[:, :col_num]
        self.tr_col_num = col_num

    def _truncate_vertically(self) -> None:
        """Remove rows, which are not to be displayed.
        Attributes affected:
            - tr_frame
            - tr_row_num
        """
        assert self.max_rows_fitted is not None
        # row_num = self.max_rows_fitted // 2
        row_num = self.max_rows_fitted
        if row_num >= 1:
            # head = self.tr_frame.iloc[:row_num, :]
            # tail = self.tr_frame.iloc[-row_num:, :]
            # self.tr_frame = concat((head, tail))
            self.tr_frame = self.tr_frame.iloc[:row_num, :]
        else:
            row_num = cast(int, self.max_rows)
            self.tr_frame = self.tr_frame.iloc[:row_num, :]
        self.tr_row_num = row_num

    def format_col(self, i: int):
        """Format column, add dtype ahead"""
        frame = self.tr_frame
        formatter = self._get_formatter(i)
        dtype = frame.iloc[:, i].dtype.name

        return [f'<{dtype}>'] + format_array(
            frame.iloc[:, i]._values,
            formatter,
            float_format=self.float_format,
            na_rep=self.na_rep,
            space=self.col_space.get(frame.columns[i]),
            decimal=self.decimal,
            leading_space=self.index,
        )


    def get_strcols(self):
        """
        Render a DataFrame to a list of columns (as lists of strings).
        """
        strcols = self._get_strcols_without_index()

        if self.index:
            #           dtype
            str_index = [""] + self._get_formatted_index(self.tr_frame)
            strcols.insert(0, str_index)

        return strcols

@dataclass
class PrintTidyverse():
    __data:DataFrame

    # def __init__(self, data=None, *args, meta=None, **kwargs):
    #     """Construct a tibble"""
    #     super().__init__(data, *args, **kwargs)
    #     self._datar = meta or {}

    def __repr__(self) -> str:
        """
        Return a string representation for a particular DataFrame.
        """
        # if self._info_repr():
        #     buf = StringIO()
        #     self.info(buf=buf)
        #     return buf.getvalue()

        # repr_params = fmt.get_dataframe_repr_params()
        # return self.to_string(**repr_params)

        ## to support pandas version prior to 1.4.0 avoiding get_dataframe_repr_params()
        from pandas.io.formats import console

        if get_option("display.expand_frame_repr"):
            line_width, _ = console.get_console_size()
        else:
            line_width = None

        repr_params =  {
            "max_rows": get_option("display.max_rows"),
            "min_rows": get_option("display.min_rows"),
            "max_cols": get_option("display.max_columns"),
            "max_colwidth": get_option("display.max_colwidth"),
            "show_dimensions": get_option("display.show_dimensions"),
            "line_width": line_width,
        }

        nr = self.nrow
        nc = self.ncol

        header_line   = f"# A tidy dataframe: {nr} X {nc}"

        from pandas import option_context

        with option_context("display.max_colwidth",repr_params["max_colwidth"]):
            row_truncated = False
            show_frame = self.__data
            if repr_params["min_rows"] and self.__data.shape[0] > repr_params["min_rows"]:
                row_truncated = True
                show_frame = self.__data.iloc[:repr_params["min_rows"], :]

            formatter = TidyDataFrameFormatter(
                # self.__data,
                show_frame,
                # min_rows=repr_params["min_rows"],
                max_rows=repr_params["max_rows"],
                max_cols=repr_params["max_cols"],
                # show_dimensions=repr_params["show_dimensions"]
            )
            formatted_str = header_line + "\n" + fmt.DataFrameRenderer(formatter)\
                                               .to_string(line_width=repr_params["line_width"])
            if row_truncated:
                footer_str = "#... with {} more rows".format(self.__data.shape[0]-repr_params["min_rows"])
                formatted_str += "\n" + footer_str

            max_footer_cols_print = 100
            if formatter.is_truncated_horizontally and formatter.tr_col_num < self.ncol:
                more_cols = self.ncol - formatter.tr_col_num
                footer_cols = ["{} <{}>".format(cname, self.__data[cname].dtype.name) 
                                  for cname in self.colnames[formatter.tr_col_num:]
                              ]
                footer_str = "{} more columns: {}".format(more_cols, ", ".join(footer_cols[0:max_footer_cols_print]))
                if more_cols > max_footer_cols_print:
                    footer_str += "..."
                if row_truncated:
                    formatted_str += ", and " + footer_str
                else:
                    formatted_str += "#... with " + footer_str
            return formatted_str

    @property
    def nrow(self):
        return self.__data.shape[0]
    
    @property
    def ncol(self):
        return self.__data.shape[1]

    @property
    def shape(self):
        return self.__data.shape

    @property
    def dim(self):
        return self.__data.shape
        
    @property
    def colnames(self):
        return list(self.__data.columns)