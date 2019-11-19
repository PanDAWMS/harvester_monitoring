import re
import os


class Errors:

    def __init__(self, filename):
        self.patterns = self.__read_patterns(filename=filename)

    # private method
    def __read_patterns(self, filename):
        """
        Read patterns from file
        """
        path = os.path.dirname(os.path.abspath(__file__))
        filename = path + '/' + filename
        try:
            with open(filename, "r") as ins:
                patterns = []
                for line in ins:
                    patterns.append(line.rstrip())
            return patterns
        except Exception as ex:
            print(ex)

    def accounting(self, errors, errors_dict):
        errors_dict[errors.key] = {}
        for error in errors.workerstats.buckets['bad']['errors']:
            isPattern = False
            error_str = " ".join((error.key).split())
            for pattern_key in self.patterns:
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
                if pattern_key.find('\\(.*?\\)') != -1:
                    pattern_key = pattern_key.replace('\\(.*?\\)', '(.*?)')
                pn = re.compile(pattern_key)
                if pn.match(r"""{0}""".format(error_str)):
                    isPattern = True
                    pattern = pn.pattern
                    break
            if isPattern:
                if pattern not in errors_dict[errors.key]:
                    errors_dict[errors.key][pattern] = {"error_count": error.doc_count,
                                                        "total_error_count": errors.workerstats.buckets[
                                                            'bad'].doc_count}
                else:
                    errors_dict[errors.key][pattern]["error_count"] = errors_dict[errors.key][
                                                                          pattern]["error_count"] + error.doc_count
            else:
                errors_dict[errors.key][error_str] = {"error_count": error.doc_count,
                                                      "total_error_count": errors.workerstats.buckets['bad'].doc_count}
        return errors_dict

    def errors_accounting_tmp(self, pq, errors, errors_dict, total_errors):
        errors_dict[pq] = {}
        for error in errors:
            isPattern = False
            error_str = " ".join((error).split())
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
                pn = re.compile(pattern_key)
                if pn.match(r"""{0}""".format(error_str)):
                    isPattern = True
                    pattern = old_pattern
                    break
            if isPattern:
                if pattern not in errors_dict[pq]:
                    errors_dict[pq][pattern] = {"error_count": errors[error], "total_error_count": total_errors}
                else:
                    errors_dict[pq][pattern]["error_count"] = errors_dict[pq][
                                                                  pattern]["error_count"] + errors[error]
            else:
                errors_dict[pq][error_str] = {"error_count": errors[error], "total_error_count": total_errors}
        return errors_dict
