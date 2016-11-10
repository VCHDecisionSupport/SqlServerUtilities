using System.Collections.Generic;
using System.Reflection;
using Varigence.Flow.FlowFramework.Validation;
using Varigence.Languages.Biml.Platform;

namespace Varigence.Languages.Biml
{
    public static class BidsHelper
    {
        public static ValidationReporter CompileBiml(Assembly workflowAssembly, string workflowResourcePath, string workflowName, IEnumerable<string> emittableBimlFileNames, IEnumerable<string> includeBimlFileNames, string targetDirectoryPath, string packageRelativePath, SqlServerVersion sqlServerVersion, SsisVersion ssisVersion, SsasVersion ssasVersion, SsisDeploymentModel ssisDeploymentModel);
    }
}

namespace BimlGenerator
{


    class Program
    {

        static void Main(string[] args)
        {
        }
    }
}