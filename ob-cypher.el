;;; ob-cypher.el --- query neo4j using cypher in org-mode blocks -*- lexical-binding: t -*-

;; Copyright (C) 2015 ZHOU Feng

;; Author: ZHOU Feng <zf.pascal@gmail.com>
;; URL: http://github.com/zweifisch/ob-cypher
;; Keywords: org babel cypher neo4j
;; Version: 0.0.1
;; Created: 8th Feb 2015
;; Package-Requires: ((s "1.9.0") (cypher-mode "0.0.6") (dash "2.19.1"))

;; This file is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation; either version 3, or (at your option)
;; any later version.

;; This file is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <http://www.gnu.org/licenses/>.

;;; Commentary:
;;
;; query neo4j using cypher in org-mode blocks
;;

;;; Code:
(require 'ob)
(require 's)
(require 'dash)
(require 'json)

(defvar org-babel-default-header-args:cypher
  '((:results . "output")))

(defvar ob-cypher/scigraph-api-key nil
  "API key for scigraph endpoint so that it can be set in a config if needed.
It is probably better to use a closure on the parameter in the org file such as
#+begin_src cypher :scigraph https://protect.server/scigraph :api-key (get-scigraph-api-key)
or similar. PLEASE DO NOT PUT YOUR API KEYS IN AN ORG FILE DIRECTLY.")

(defun ob-cypher/parse-result (output)
  (->> (s-lines output)
    (-filter (-partial 's-starts-with? "|"))
    (-map (-partial 's-chop-suffix "|"))
    (-map (-partial 's-chop-prefix "|"))
    (-map (-partial 's-split " | "))))

(defun ob-cypher/table (output)
  (org-babel-script-escape (ob-cypher/parse-result output)))

(defun ob-cypher/property (property)
  (format "%s: %s" (car property) (cdr property)))

(defun ob-cypher/node-to-dot (node)
  (let ((labels (cdr (assoc 'labels node))))
    (s-format "n${id} [label=\"{${head}<body> ${body}}\"]" 'aget
              `(("id" . ,(cdr (assoc 'id node)))
                ("head" . ,(if (> (length labels) 0) (concat "<head>" (s-join ":" labels) "|") ""))
                ("body" . ,(s-join "\\n" (-map 'ob-cypher/property (cdr (assoc 'properties node)))))))))

(defun ob-cypher/rel-to-dot (rel)
  (s-format "n${start} -> n${end} [label = ${label}]" 'aget
            `(("start" . ,(cdr (assoc 'startNode rel)))
              ("end" . ,(cdr (assoc 'endNode rel)))
              ("label" . ,(cdr (assoc 'type rel))))))

(defun ob-cypher/json-to-dot (output)
  (let* ((parsed (json-read-from-string output))
         (results (cdr (assoc 'results parsed)))
         (data (if (> (length results) 0)
                   (cdr (assoc 'data (elt results 0)))))
         (graphs (-map (lambda (graph) (cdr (assoc 'graph graph)))
                       data))
         (rels (-mapcat
                (lambda (graph)
                  (append (cdr (assoc 'relationships graph)) nil))
                graphs))
         (nodes (-mapcat
                 (lambda (graph)
                   (append (cdr (assoc 'nodes graph)) nil))
                 graphs)))
    (s-format "digraph {\nnode[shape=Mrecord]\n${nodes}\n${rels}\n} " 'aget
              `(("nodes" . ,(s-join "\n" (-map 'ob-cypher/node-to-dot nodes)))
                ("rels" . ,(s-join "\n" (-map 'ob-cypher/rel-to-dot rels)))))))

(defun ob-cypher/json-to-table (output)
  (let* ((json-array-type 'list)
	       (parsed (json-read-from-string output))
         (results (cdr (assoc 'results parsed)))
         (data (if (> (length results) 0)
                   (cdr (assoc 'data (elt results 0)))))
	       (columns (if (> (length results) 0)
                      (cdr (assoc 'columns (elt results 0)))))
         (rows (-map (lambda (row) (cdr (assoc 'row row))) data)))
    (cons columns (cons 'hline rows))))


(defun ob-cypher/query (statement host port authstring)
  (let* ((statement (s-replace "\"" "\\\"" statement))
         (body (format "{\"statements\":[{\"statement\":\"%s\",\"resultDataContents\":[\"graph\",\"row\"]}]}"
                       (s-join " " (s-lines statement))))
         (url (format "http://%s:%d/db/data/transaction/commit" host port))
         (tmp (org-babel-temp-file "cypher-curl-"))
         (cmd (format "curl -sH 'Accept: application/json; charset=UTF-8' -H 'Content-Type: application/json' -H 'Authorization: Basic %s' -d@'%s' '%s'" authstring tmp url)))
    (message cmd)
    (with-temp-file tmp
      (insert body))
    (shell-command-to-string cmd)))

(defun ob-cypher/dot (statement host port output authstring)
  (let* ((tmp (org-babel-temp-file "cypher-dot-"))
         (result (ob-cypher/query statement host port authstring))
         (dot (ob-cypher/json-to-dot result))
         (cmd (format "dot -T%s -o %s %s" (file-name-extension output) output tmp)))
    (message result)
    (message dot)
    (message cmd)
    (with-temp-file tmp
      (insert dot))
    (org-babel-eval cmd "")
    nil))

(defun ob-cypher/rest (statement host port authstring)
  (let* ((tmp (org-babel-temp-file "cypher-rest-"))
         (result (ob-cypher/query statement host port authstring))
         (tbl (ob-cypher/json-to-table result)))
    (message result)
    tbl))

(defun ob-cypher/shell (statement host port result-type)
  (let* ((tmp (org-babel-temp-file "cypher-shell-"))
         (cmd (s-format "neo4j-shell -host ${host} -port ${port} -file ${file}" 'aget
                        `(("host" . ,host)
                          ("port" . ,(int-to-string port))
                          ("file" . ,tmp))))
         (result (progn (with-temp-file tmp (insert statement))
                        (shell-command-to-string cmd))))
    (message cmd)
    (if (string= "output" result-type) result (ob-cypher/table result))))

;;; support for issuing cypher queries to SciGraph

(defun ob-cypher/scigraph/dot (statement vars scigraph limit output api-key)
  (let* ((tmp (org-babel-temp-file "cypher-dot-"))
         (result (ob-cypher/scigraph/query statement vars scigraph limit api-key))
         (dot (ob-cypher/scigraph/json-to-dot result))
         (cmd (format
               (concat
                "dot "
                "-Efontname='Dejavu Sans Mono' "
                "-Nfontname='Dejavu Sans Mono' "
                "-Grankdir=LR "
                "-T%s -o %s %s")
               (file-name-extension output) output tmp)))
    (with-temp-file tmp
      (insert dot))
    (org-babel-eval cmd "")
    nil))

(defun ob-cypher/scigraph/json-to-dot (result)
  (let* ((json-array-type 'list)
         (json-object-type 'hash-table)
         (parsed (json-read-from-string result))
         (edges (gethash "edges" parsed))
         (nodes (gethash "nodes" parsed))
         (template `(("nodes" . ,(s-join "\n" (-map 'ob-cypher/scigraph/node-to-dot nodes)))
                     ("edges" . ,(s-join "\n" (-map 'ob-cypher/scigraph/edge-to-dot edges))))))
    (s-format "digraph {\nnode[shape=Mrecord]\n${nodes}\n${edges}\n} " 'aget template)))

(defun ob-cypher/scigraph/safe-id (id)
  (replace-regexp-in-string "-" "_" ; FIXME dry
  (replace-regexp-in-string "\\." "_"
  (replace-regexp-in-string "/" "_"
  (replace-regexp-in-string ":" "_" id)))))

(defun ob-cypher/scigraph/node-to-dot (node)
  (let ((sid (ob-cypher/scigraph/safe-id (gethash "id" node)))
        (label (or (gethash "lbl" node) ; can't have nil here or `s-format' fails
                   "MISSING LABEL")))
    (s-format
     "n${id} [label=\"${label}\\n${id-real}\"]" 'aget
     `(("id" . ,sid)
       ("id-real" ,(gethash "id" node))
       ("label" . ,label)))))

(defun ob-cypher/scigraph/edge-to-dot (edge)
  (s-format "n${start} -> n${end} [label = \"${label}\"]" 'aget
            `(("start" . ,(ob-cypher/scigraph/safe-id (gethash "sub" edge)))
              ("end" . ,(ob-cypher/scigraph/safe-id (gethash "obj" edge)))
              ("label" . ,(gethash "pred" edge)))))

(defun ob-cypher/scigraph/rest (statement vars scigraph limit result-params label-predicate api-key)
  (let* ((tmp (org-babel-temp-file "cypher-rest-"))
         (result (ob-cypher/scigraph/query statement vars scigraph limit api-key)))
    (if (member "drawer" result-params)
        (let ((jpp (with-temp-buffer
                     (insert result)
                     (json-pretty-print-buffer)
                     (buffer-string))))
          jpp)
      (ob-cypher/scigraph/json-to-table result label-predicate))))

(defun ob-cypher/scigraph--get-api-key (api-key sep)
  "Return url parameter embedding API-KEY if API-KEY is non-nil.
If API-KEY is t use the value of `ob-cypher/scigraph-api-key'.
If API-KEY is a symbol or a string the value will be used as the key."
  (message "tok: %S %S" (type-of api-key) api-key)
  (let ((key (and api-key
                  (or (and (stringp api-key) api-key)
                      (and (booleanp api-key) ob-cypher/scigraph-api-key)
                      (and (symbolp api-key) (symbol-name api-key))))))
    (when key
      (concat sep "key=" key))))

(defun ob-cypher/scigraph/query (statement vars scigraph limit api-key)
  (let* ((qs (format "?limit=%s&cypherQuery=%s%s"
                     (or limit 10)
                     (url-hexify-string statement)
                     (let ((qp (string-join
                                (mapcar (lambda (pair) (format "%s=%s" (car pair) (cdr pair))) vars) "&")))
                       (if (string= qp "" ) qp (concat "&" qp)))))
         (url (concat scigraph "/cypher/execute.json" qs (ob-cypher/scigraph--get-api-key api-key "&")))
         (cmd (format "curl -sH 'Accept: application/json; charset=UTF-8' '%s'" url)))
    ;;(message "%s" cmd) ; XXX
    (message "query wat: %s..." (substring cmd 0 (min 200 (length cmd))))
    (shell-command-to-string cmd)))

(defun ob-cypher/scigraph/curies (scigraph api-key) ; XXX unused
  ;; example: (ob-cypher/scigraph/curies "http://selene:9000/scigraph")
  (let* ((json-array-type 'list)
         (json-object-type 'hash-table)
         (url (concat scigraph "/cypher/curies" (ob-cypher/scigraph--get-api-key api-key "?")))
         (result (shell-command-to-string
                  (format "curl -sH 'Accept: application/json; charset=UTF-8' '%s'" url)))
         (parsed (json-read-from-string result)))
    parsed))

(defun ob-cypher/scigraph/json-to-table (result &optional label-predicate)
  ;; {"nodes": [{} ...], "edges": [{} ...]}
  (let* ((json-array-type 'list)
         (json-object-type 'hash-table)
         (parsed (json-read-from-string result))
         (nodes (gethash "nodes" parsed))
         (rows (cl-loop for node in nodes
                        collect (list (gethash "id" node)
                                      (or (and label-predicate
                                               (car (gethash label-predicate (gethash "meta" node))))
                                          (gethash "lbl" node) ""))))
         (header (list "id" "label")))
    (cons header (cons 'hline rows))))

;;; execute

(defun org-babel-execute:cypher (body params)
  (let* ((host (or (cdr (assoc :host params)) "127.0.0.1"))
         (port (or (cdr (assoc :port params)) 1337))
         (username (or (cdr (assoc :username params)) "neo4j"))
         (password (or (cdr (assoc :password params)) "neo4j"))
         (authstring (base64-encode-string (concat username ":" password)))
         (http-port (or (cdr (assoc :http-port params)) 7474))
         (scigraph (cdr (assoc :scigraph params)))
         ;; XXX can't distinguish :api-key :var from :api-key (or)
         ;; :var so we are stuck requiring an explicit value, not just
         ;; the presence of the key
         (api-key (cdr (assoc :api-key params)))
         (limit (cdr (assoc :limit params)))
         (label-predicate (cdr (assoc :label params)))
         (vars (org-babel--get-vars params))
         (result-params (cdr (assoc :result-params params)))
         (result-type (cdr (assoc :result-type params)))
         (output (cdr (assoc :file params)))
         (body (if (s-ends-with? ";" body) body (s-append ";" body))))
    (if scigraph
        (if (and output (member "file" result-params))
            (ob-cypher/scigraph/dot body vars scigraph limit output api-key)
          (ob-cypher/scigraph/rest body vars scigraph limit result-params label-predicate api-key))
      (if (and output (member "file" result-params))
          (ob-cypher/dot body host http-port output authstring)
        (ob-cypher/rest body host http-port authstring)))))

(provide 'ob-cypher)
;;; ob-cypher.el ends here
