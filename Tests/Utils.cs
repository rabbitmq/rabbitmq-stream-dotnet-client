using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class Utils<TResult>
    {
        private readonly ITestOutputHelper testOutputHelper;

        public Utils(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
        }
        
        // Added this generic function to trap error during the 
        // tests
        public void WaitUntilTaskCompletes(TaskCompletionSource<TResult> tasks, 
            bool expectToComplete = true,
            int timeOut = 10000)
        {
            try
            {
                var resultTestWait = tasks.Task.Wait(timeOut);
                Assert.Equal(resultTestWait, expectToComplete );
            }
            catch (Exception e)
            {
                testOutputHelper.WriteLine($"wait until task completes error #{e}");
                throw;
            }
        }
    }
}