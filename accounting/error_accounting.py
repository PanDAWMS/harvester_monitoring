import re
import os

from datetime import datetime, timedelta
from .clusterlogs import pipeline


class Errors:

    def __init__(self, filename):
        self.filename = filename
        self.patterns = self.__read_patterns()

    # private method
    def __read_patterns(self):
        """
        Read patterns from file
        """
        path = os.path.dirname(os.path.abspath(__file__))

        fullpath = path + '/' + self.filename

        try:
            with open(fullpath, "r") as ins:
                patterns = set()
                for line in ins:
                    patterns.add(line.rstrip())
            return patterns
        except Exception as ex:
            print(ex)

    def errors_accounting(self, element, errors, errors_dict, total_errors):
        errors_dict[element] = {}
        # pats = self.returnRegexList()

        for error in errors:
            isPattern = False
            error_str = " ".join((error).split())
            # if any(regex.match(error_str) for regex in pats):
            #     isPattern = True
            #
            #     break
            for pattern_key in self.patterns:
                old_pattern = pattern_key

                if pattern_key.find('\\') != -1:
                    pattern_key = pattern_key.replace('\\', '\\\\')
                if pattern_key.find('/') != -1:
                    pattern_key = pattern_key.replace('/', '\/')
                if pattern_key.find('[') != -1:
                    pattern_key = pattern_key.replace('[', '\[')
                if pattern_key.find(']') != -1:
                    pattern_key = pattern_key.replace(']', '\]')
                if pattern_key.find('(') != -1:
                    pattern_key = pattern_key.replace('(', '\(')
                if pattern_key.find(')') != -1:
                    pattern_key = pattern_key.replace(')', '\)')
                if pattern_key.find('\(.*?\)') != -1:
                    pattern_key = pattern_key.replace('\(.*?\)', '(.*?)')
                if pattern_key.find('\(.*\)') != -1:
                    pattern_key = pattern_key.replace('\(.*\)', '(.*)')
                if pattern_key.find('+') != -1:
                    pattern_key = pattern_key.replace('+', '')

                pn = re.compile(pattern_key)

                if pn.match(r"""{0}""".format(error_str)):
                    isPattern = True
                    pattern = old_pattern
                    break

            if isPattern:
                if pattern not in errors_dict[element]:
                    errors_dict[element][pattern] = {"error_count": errors[error], "total_error_count": total_errors}
                else:
                    errors_dict[element][pattern]["error_count"] = errors_dict[element][
                                                                  pattern]["error_count"] + errors[error]
            else:
                errors_dict[element][error_str] = {"error_count": errors[error], "total_error_count": total_errors}
        return errors_dict

    def write_patterns(self, errors_df, clustering_type='SIMILARITY', mode='create', model_name='harvester_tmp.model'):
        """
        Write patterns to file
        """
        path = os.path.dirname(os.path.abspath(__file__))
        fullpath = path + '/' + self.filename

        if len(self.patterns) == 0:
            patterns = set()
        else:
            patterns = self.patterns

        file_name = 'data_sample-{0}.csv'.format(str(datetime.utcnow()))
        errors_df.to_csv(path+'/datasamples/' + clustering_type + '_' + file_name, sep='\t', encoding='utf-8')
        print(file_name)

        try:
            cluster = pipeline.Chain(errors_df, 'message',  mode=mode,
                                     model_name=path+'/models/'+model_name, matching_accuracy=0.8,
                                     clustering_type=clustering_type, add_placeholder=True)
            cluster.process()
        except:
            pass
        new_patterns_list = list(cluster.result['pattern'].values)

        for pattern in new_patterns_list:
            if str(pattern).find('(.*?)') != -1:
                pl_index = 0
                dub_pattern_candidate = str(pattern).split(' ')
                for i, item in enumerate(dub_pattern_candidate):
                    if item == '(.*?)':
                        pl_index = i
                    if i < len(dub_pattern_candidate) - 1:
                        if dub_pattern_candidate[pl_index] == dub_pattern_candidate[i+1]:
                            del dub_pattern_candidate[i+1]
                        else:
                            pl_index = i
                if len(pattern) > len('(.*?)')+2:
                    patterns.add(' '.join(dub_pattern_candidate))

            # left_s = str(pattern).find('｟')
            # right_s = str(pattern).find('｠')
            # if (left_s) != -1:
            #     if (right_s) != -1:
            #         patterns.add(str(pattern).replace('｟*｠', '(.*?)'))
        with open(fullpath, 'w') as f:
            for pattern in patterns:
                f.write("%s\n" % pattern)
        self.patterns = patterns