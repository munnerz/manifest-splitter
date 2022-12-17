package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"log"
	"os"
	"path/filepath"
	"strings"

	flag "github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"

	"github.com/munnerz/manifest-splitter/discovery"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

var (
	kubeconfig  string
	outputDir   string
	expandLists bool

	scheme = runtime.NewScheme()
)

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a KUBECONFIG file used to lookup discovery information")
	flag.StringVar(&outputDir, "output", "config/", "Path to a directory where output files will be written")
	flag.BoolVar(&expandLists, "expand-lists", true, "if true, List-like resources will be expanded into multiple YAML files")
}

// manifest-splitter ingests Kubernetes manifest files and outputs a directory
// structure that splits the resources into cluster & namespace scoped groups.
//
// This is useful when managing an Anthos Config Management configuration
// repository, or otherwise to simply inspect what namespaces a given set of
// Kubernetes manifests will be installed into.

func main() {
	flag.Parse()

	restcfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatalf("Failed to build kubernetes REST client config: %v", err)
	}

	inspector, err := discovery.NewAPIServerResourceInspector(restcfg)
	if err != nil {
		log.Fatalf("Failed to construct APIServer backed resource inspector: %v", err)
	}

	// accumulated map of input filename to sets of resources
	files := make(map[string][]resource)
	inputs := flag.Args()
	for _, input := range inputs {
		log.Printf("Reading input file %q", input)
		// begin code that needs repeating
		r, err := os.Open(input)
		if err != nil {
			log.Fatalf("Failed to read input file: %v", err)
		}

		resources, err := decodeResourceManifest(input, r)
		if err != nil {
			log.Fatalf("Failed to decode input file: %v", err)
		}

		log.Printf("Found %d resources in file %q", len(resources), input)
		files[input] = resources
	}

	if err := populateNamespacedField(inspector, files); err != nil {
		log.Fatalf("Error discovering whether resources are namespaced: %v", err)
	}

	if err := validateResourceFiles(files); err != nil {
		log.Fatalf("Error validating input files: %v", err)
	}

	// gather output resources
	// outputs maps namespace->resources
	outputs := make(map[string][]resource)
	for _, resources := range files {
		for _, resource := range resources {
			log.Printf("Processing resource %q", resource.obj.GetName())
			ns := resource.obj.GetNamespace()
			if resource.obj.IsList() {
				log.Printf("Encountered list in file %q", resource.inputFilename)
				ns = resource.listNamespaceName
			}
			if resource.obj.GetKind() == "Namespace" && resource.obj.GetAPIVersion() == "v1" {
				ns = resource.obj.GetName()
			}
			list := outputs[ns]
			list = append(list, resource)
			outputs[ns] = list
		}
	}

	// write output resources to directory
	for ns, resources := range outputs {
		log.Printf("Writing output namespace: %q", ns)
		dirname := filepath.Join(outputDir, "namespaces", ns)
		if ns == "" {
			dirname = filepath.Join(outputDir, "cluster")
		}
		if err := os.MkdirAll(dirname, 0755); err != nil {
			log.Fatalf("Error creating output directory: %v", err)
		}

		log.Printf("Writing resources in directory: %q", dirname)
		for _, resource := range resources {
			dir := dirname
			if resource.obj.GetKind() == "Repo" && resource.obj.GetAPIVersion() == "configmanagement.gke.io/v1" {
				dir = filepath.Join(outputDir, "system")
				if err := os.MkdirAll(dir, 0755); err != nil {
					log.Fatalf("Error creating output directory: %v", err)
				}
			}
			filename := resourceFilename(resource)
			outputfile := filepath.Join(dir, filename)
			log.Printf("Writing resource %q in namespace %q to: %s", resource.obj.GetName(), ns, outputfile)
			if err := ioutil.WriteFile(outputfile, resource.data, 0644); err != nil {
				log.Fatalf("Error writing output file %q: %v", outputfile, err)
			}
		}
	}
}

func resourceFilename(r resource) string {
	if r.obj.IsList() {
		inputFileName := filepath.Base(r.inputFilename)
		inputFileNameStripped := strings.TrimSuffix(inputFileName, filepath.Ext(inputFileName))
		return fmt.Sprintf("%s-%d-%s.%s", r.obj.GetKind(), r.idx, inputFileNameStripped, r.format)
	}
	if r.obj.GetKind() == "Namespace" && r.obj.GetAPIVersion() == "v1" {
		return fmt.Sprintf("namespace.%s", r.format)
	}

	return fmt.Sprintf("%s-%s.%s", r.obj.GetKind(), r.obj.GetName(), r.format)
}

func populateNamespacedField(inspector discovery.ResourceInspector, files map[string][]resource) error {
	for inputFilename, resources := range files {
		for i, resource := range resources {
			gvk := resource.obj.GroupVersionKind()
			isNamespaced, err := inspector.IsNamespaced(gvk)
			if err != nil {
				return fmt.Errorf("in input file %q: %v", inputFilename, err)
			}
			resources[i].namespaced = isNamespaced
		}
	}
	return nil
}

func validateResourceFiles(files map[string][]resource) error {
	type namespacedName struct{ name, namespace string }
	alreadyContains := func(list []namespacedName, toFind namespacedName) bool {
		for _, e := range list {
			if toFind == e {
				return true
			}
		}
		return false
	}

	existingResources := make(map[schema.GroupKind][]namespacedName)
	for _, resources := range files {
		if err := validateResources(resources); err != nil {
			return err
		}

		for _, resource := range resources {
			gk := resource.obj.GroupVersionKind().GroupKind()
			existingNamespacedNames := existingResources[gk]
			nn := namespacedName{namespace: resource.obj.GetNamespace(), name: resource.obj.GetName()}
			// find resources with duplicate names
			if alreadyContains(existingNamespacedNames, nn) {
				return fmt.Errorf("found duplicate resource %s/%s with group/kind %q", resource.obj.GetNamespace(), resource.obj.GetName(), gk.String())
			}
		}
	}

	return nil
}

func validateResources(resources []resource) error {
	for i := range resources {
		// pass a reference to the item in the list so validateResource can
		// modify it to set the 'listNamespaceName' if necessary.
		if err := validateResource(&resources[i]); err != nil {
			return err
		}
	}
	return nil
}

func validateResource(r *resource) error {
	if r.obj.IsList() {
		return validateResourceList(r)
	}

	if r.namespaced && r.obj.GetNamespace() == "" {
		return fmt.Errorf("namespaced resource %q missing metadata.namespace field", r)
	}
	if !r.namespaced && r.obj.GetNamespace() != "" {
		r.obj.SetNamespace("")
		//return fmt.Errorf("non-namespaced resource %q specifies metadata.namespace field", r)
	}

	return nil
}

// validateResourceList ensures that the given resource, which must be a 'List'
// has valid list members.
// This includes ensuring that all resources in the list share the same
// namespace, as well as regular resource validation performed on list items.
func validateResourceList(r *resource) error {
	if !r.obj.IsList() {
		return fmt.Errorf("non-list resource passed to validateResourceList")
	}

	ns := ""
	declaredNamespaces := map[string]struct{}{}
	// validate each item in the list
	if err := r.obj.EachListItem(func(obj runtime.Object) error {
		// make a copy of the resource
		inner := &resource{
			idx:               r.idx,
			inputFilename:     r.inputFilename,
			data:              r.data,
			format:            r.format,
			obj:               obj.(*unstructured.Unstructured),
			namespaced:        r.namespaced,
			listNamespaceName: r.listNamespaceName,
		}
		// ensure that all resources have the same namespace
		declaredNamespaces[inner.obj.GetNamespace()] = struct{}{}
		if len(declaredNamespaces) > 1 {
			return fmt.Errorf("found more than one namespace declared in resources in a single list in file %q: %v", r.inputFilename, declaredNamespaces)
		}

		ns = inner.obj.GetNamespace()
		return validateResource(inner)
	}); err != nil {
		return err
	}

	// set the listNamespaceName
	r.listNamespaceName = ns
	return nil
}

type format string

const (
	jsonFormat format = "json"
	yamlFormat format = "yaml"
)

type resource struct {
	// idx is the index of the resource in the manifest input file.
	// this is used to name the output file if a resource is a list, as
	// lists don't have declared names.
	idx           int
	inputFilename string

	data       []byte
	format     format
	obj        *unstructured.Unstructured
	namespaced bool

	// listNamespaceName is only used if obj.IsList() == true.
	// It is the namespace of the items contained in the list.
	listNamespaceName string
}

// decoder is a type that encapsulates decoding into an object whilst also
// returning the bytes read whilst decoding.
type decoder func(r io.Reader, into interface{}) ([]byte, error)
type encoder func(interface{}) ([]byte, error)

func decodeResourceManifest(input string, r io.Reader) ([]resource, error) {
	r, _, isJSON := utilyaml.GuessJSONStream(r, 4096)
	var decode decoder
	var encode encoder
	var format format
	if !isJSON {
		decode = DecodeYAML
		encode = EncodeYAML
		format = yamlFormat
	} else {
		decode = DecodeJSON
		encode = EncodeJSON
		format = jsonFormat
	}

	idx := 0
	var resources []resource
	for {
		u := unstructured.Unstructured{}
		bytes, err := decode(r, &u)
		if err == io.EOF {
			return resources, nil
		}
		if err != nil {
			return nil, err
		}
		// skip empty/invalid resources
		if u.GetAPIVersion() == "" || u.GetKind() == "" {
			continue
		}

		if expandLists && u.IsList() {
			u.EachListItem(func(obj runtime.Object) error {
				u := obj.(*unstructured.Unstructured)
				data, err := encode(u)
				if err != nil {
					return err
				}
				resources = append(resources, resource{
					idx:           idx,
					inputFilename: input,
					data:          data,
					format:        format,
					obj:           u,
				})
				idx++
				return nil
			})
			continue
		}

		resources = append(resources, resource{
			idx:           idx,
			inputFilename: input,
			data:          bytes,
			format:        format,
			obj:           &u,
		})
		idx++
	}

	return resources, nil
}

// Decode reads a YAML document as JSON from the stream or returns
// an error. The decoding rules match json.Unmarshal, not
// yaml.Unmarshal.
func DecodeYAML(r io.Reader, into interface{}) ([]byte, error) {
	buffer := bufio.NewReader(r)
	yamlReader := utilyaml.NewYAMLReader(buffer)
	bytes, err := yamlReader.Read()
	if err != nil && err != io.EOF {
		return nil, err
	}

	if len(bytes) != 0 {
		err := yaml.Unmarshal(bytes, into)
		if err != nil {
			return nil, err
		}
	}

	return bytes, err
}

func EncodeYAML(obj interface{}) ([]byte, error) {
	return yaml.Marshal(obj)
}

func DecodeJSON(r io.Reader, into interface{}) ([]byte, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	// return EOF if we've not read any data
	if len(data) == 0 {
		return nil, io.EOF
	}

	return data, json.Unmarshal(data, into)
}

func EncodeJSON(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}
